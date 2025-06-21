// main.rs
// This file now sets up an HTTP server to expose the S3-like service.

mod bucket; // Declare the bucket module
mod handlers;
mod object;
mod s3_service; // Declare the s3_service module
mod storage;
mod structs;

use actix_web::http::StatusCode;
use actix_web::http::header::ContentType;
use actix_web::web;
use actix_web::{App, HttpResponse, HttpServer, error::ResponseError};
use handlers::{
    create_bucket_handler, delete_bucket_handler, delete_object_handler, get_object_handler,
    list_buckets_handler, list_objects_handler, put_object_handler,
};
use s3_service::{S3Error, S3Service};
use std::sync::{Arc, Mutex};
use storage::Storage;
use tracing::{error, info};
use tracing_actix_web::TracingLogger;
use tracing_subscriber::{EnvFilter, fmt};

// Initialize tracing
fn init_logging() {
    // Initialize tracing with JSON formatter
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        // .pretty()
        .with_file(false)
        .with_line_number(false)
        .with_target(false)
        .init();
}

// --- Helper function to map S3Error to Actix Web HTTP responses ---
impl ResponseError for S3Error {
    fn error_response(&self) -> HttpResponse {
        let status = self.status_code();
        let error_message = self.to_string();

        HttpResponse::build(status)
            .insert_header(ContentType::json())
            .json(serde_json::json!({
                "error": error_message,
                "code": status.as_u16()
            }))
    }

    fn status_code(&self) -> StatusCode {
        match self {
            S3Error::BucketAlreadyExists(_) => StatusCode::CONFLICT,
            S3Error::BucketNotFound(_) => StatusCode::NOT_FOUND,
            S3Error::ObjectNotFound(_, _) => StatusCode::NOT_FOUND,
            S3Error::BucketOperationFailed(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// The main function is now asynchronous and sets up the Actix Web server.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    init_logging();

    info!("Starting S3-like Storage HTTP API on http://127.0.0.1:8080");

    // Initialize Storage first
    let db_path = "s3_storage.db";
    let storage = match Storage::new(db_path) {
        Ok(s) => Arc::new(Mutex::new(s)),
        Err(e) => {
            error!("Failed to initialize storage: {}", e);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Storage initialization failed: {}", e),
            ));
        }
    };

    // Initialize S3Service by passing it the storage Arc
    let s3_service = Arc::new(Mutex::new(S3Service::new(storage.clone())));

    HttpServer::new(move || {
        // Only provide s3_service_data to the app_data.
        // Handlers will interact with S3Service, which internally manages Storage.
        let s3_service_data = web::Data::new(s3_service.clone());

        App::new()
            .wrap(TracingLogger::default())
            .app_data(s3_service_data.clone())
            .service(
                web::resource("/buckets/{bucket_name}")
                    .put(create_bucket_handler) // create_bucket_handler no longer needs 'storage' directly
                    .delete(delete_bucket_handler),
            )
            .service(web::resource("/buckets").get(list_buckets_handler))
            .service(
                web::resource("/buckets/{bucket_name}/objects/{object_key}")
                    .put(put_object_handler)
                    .get(get_object_handler)
                    .delete(delete_object_handler),
            )
            .service(web::resource("/buckets/{bucket_name}/objects").get(list_objects_handler))
            .default_service(web::to(|| async { HttpResponse::NotFound().finish() }))
    })
    .bind(("127.0.0.1", 8080))?
    .workers(5)
    .run()
    .await
}
