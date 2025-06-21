// main.rs
// This file now sets up an HTTP server to expose the S3-like service.

mod bucket; // Declare the bucket module
mod object;
mod s3_service; // Declare the s3_service module
mod storage;

use storage::Storage;
use actix_web::HttpRequest;
use actix_web::http::StatusCode;
use actix_web::http::header::CONTENT_TYPE;
use actix_web::{App, HttpResponse, HttpServer, error::ResponseError, web::Bytes};
use actix_web::web;
use std::sync::{Arc, Mutex};
use s3_service::{S3Error, S3Service};
use serde::Serialize;
use std::collections::HashMap;
// For JSON responses
use tracing::{error, info};
use tracing_actix_web::TracingLogger;
use tracing_subscriber::{EnvFilter, fmt};
use storage::StorageError;
use crate::bucket::BucketError;
use crate::object::Object;

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
        HttpResponse::build(self.status_code())
            .insert_header(actix_web::http::header::ContentType::json())
            .json(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match self {
            S3Error::BucketAlreadyExists(_) => StatusCode::CONFLICT,
            S3Error::BucketNotFound(_) => StatusCode::NOT_FOUND,
            S3Error::ObjectNotFound(_, _) => StatusCode::NOT_FOUND,
            S3Error::ObjectCreationFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
            S3Error::BucketOperationFailed(err) => match err {
                BucketError::StorageError(err) => match err {
                    StorageError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    StorageError::IoError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    StorageError::SystemTimeError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    StorageError::JsonError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    StorageError::TransactionCommitError => StatusCode::INTERNAL_SERVER_ERROR,
                    StorageError::InvalidPath(_) => StatusCode::INTERNAL_SERVER_ERROR,
                    StorageError::ObjectNotFound(_, _) => StatusCode::NOT_FOUND,
                },
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}

// --- Request/Response Structs (for JSON where applicable) ---

// For listing buckets or objects
#[derive(Serialize)]
struct ListResponse {
    items: Vec<String>,
}

#[derive(Serialize)]
struct BucketCreatedResponse {
    name: String,
    message: String,
}

#[derive(Serialize)]
struct BucketDeletedResponse {
    message: String,
    bucket: String,
}

#[derive(Serialize)]
struct ObjectCreatedResponse<'a> {
    name: String,
    bucket: String,
    metadata: &'a Object, // Assuming Object can be serialized this way
    message: String,
}

#[derive(Serialize)]
struct ObjectDeletedResponse {
    name: String,
    bucket: String,
    message: String,
}

#[derive(Serialize)]
struct ObjectListResponse {
    bucket: String,
    items: Vec<String>,
}

#[derive(Serialize)]
struct BucketNotFoundResponse {
    message: String,
    bucket: String,
}

// #[derive(Serialize)]
// struct ObjectNotFoundResponse {
//     name: String,
//     bucket: String,
//     message: String,
// }

#[derive(Serialize)]
struct ObjectCreateFailedResponse {
    name: String,
    bucket: String,
    message: String,
}

// --- Handler Functions for API Endpoints ---

/// Handles PUT /buckets/{bucket_name}
/// Creates a new bucket.
async fn create_bucket_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    storage: web::Data<Arc<Mutex<Storage>>>, // `web::Data` wraps the Arc<Mutex<Storage>>
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let mut s3 = s3_service.lock().unwrap(); // Acquire mutex lock on S3Service

    // IMPORTANT FIX: Unwrapping web::Data and cloning the inner Arc
    match s3.create_bucket(&bucket_name, storage.get_ref().clone()) {
        Ok(_) => {
            info!("Bucket '{}' created.", bucket_name);
            Ok(HttpResponse::Created().json(BucketCreatedResponse {
                name: bucket_name,
                message: "Bucket created successfully".to_string(),
            }))
        }
        Err(e) => {
            error!(error = %e, "Failed to create bucket");
            Err(e)
        }
    }
}

/// Handles DELETE /buckets/{bucket_name}
/// Deletes an existing bucket.
async fn delete_bucket_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let mut s3 = s3_service.lock().unwrap();
    match s3.delete_bucket(&bucket_name) {
        Ok(_) => {
            info!("Bucket '{}' deleted.", bucket_name);
            Ok(HttpResponse::NoContent().json(BucketDeletedResponse {
                message: "Bucket deleted successfully".to_string(),
                bucket: bucket_name,
            }))
        }
        Err(e) => {
            error!(error = %e, "Failed to delete bucket");
            Err(e)
        }
    }
}

/// Handles GET /buckets
/// Lists all existing buckets.
async fn list_buckets_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
) -> Result<HttpResponse, S3Error> {
    let s3 = s3_service.lock().unwrap();
    let buckets = s3.list_buckets();
    Ok(HttpResponse::Ok().json(ListResponse { items: buckets }))
}

/// Handles PUT /buckets/{bucket_name}/objects/{object_key}
/// Puts an object into a bucket. The object data is taken from the request body.
#[tracing::instrument(
    name = "Put object",
    skip(s3_service, body, req),
    fields(
        bucket = %path.0,
        object_key = %path.1,
        object_size = body.len()
    )
)]
async fn put_object_handler(
    req: HttpRequest,
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
    body: Bytes, // Raw bytes from the request body
) -> Result<HttpResponse, S3Error> {
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok()) // Safer way to get Option<String> from header
        .map(|s| s.to_string());

    let user_metadata = req
        .headers()
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-user-meta-"))
        .filter_map(|(k, v)| { // Use filter_map for cleaner Option handling
            v.to_str().ok().map(|val_str| {
                (
                    k.as_str().strip_prefix("x-user-meta-").unwrap_or(k.as_str()).to_string(), // Strip prefix for user-facing metadata keys
                    val_str.to_string(),
                )
            })
        })
        .collect::<HashMap<_, _>>();

    let (bucket_name, object_key) = path.into_inner();
    info!(
        "Put object: bucket={}, object_key={}",
        bucket_name, object_key
    );
    
    // Create the Object instance
    let object_to_put = Object::new(
        object_key.clone(), // Clone for the Object constructor
        body.to_vec(),
        content_type,
        Some(user_metadata),
    )?; // Propagate ObjectError as S3Error::ObjectCreationFailed

    let mut s3 = s3_service.lock().unwrap();
    
    // S3Service::put_object now returns an owned Object
    match s3.put_object(
        &bucket_name,
        object_to_put, // Pass the owned object
    ) {
        Ok(returned_object) => { // Now receive an owned Object
            info!("Object '{}' put into bucket '{}'.", returned_object.key, bucket_name);
            Ok(HttpResponse::Created().json(ObjectCreatedResponse {
                name: returned_object.key.clone(),
                bucket: bucket_name,
                metadata: &returned_object, // Pass a reference to the returned object
                message: "Object created successfully".to_string(),
            }))
        }
        Err(e) => {
            error!(error = %e, "Failed to store object");
            Ok(HttpResponse::InternalServerError().json(ObjectCreateFailedResponse {
                name: object_key,
                bucket: bucket_name,
                message: "Object creation failed".to_string(),
            }))
        }
    }
}

/// Handles GET /buckets/{bucket_name}/objects/{object_key}
/// Retrieves an object from a bucket.
#[tracing::instrument(
    name = "Get object",
    skip(s3_service),
    fields(
        bucket = %path.0,
        object_key = %path.1
    )
)]
async fn get_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let s3 = s3_service.lock().unwrap();
    match s3.get_object(&bucket_name, &object_key) {
        Ok(object) => {
            info!(
                "Object '{}' retrieved from bucket '{}'.",
                object_key, bucket_name
            );
            let mut response = HttpResponse::Ok();
            if let Some(content_type) = &object.content_type {
                response.insert_header((CONTENT_TYPE, content_type.as_str()));
            }
            Ok(response.body(object.data)) // Pass the owned Vec<u8> directly
        }
        Err(e) => {
            error!(error = %e, "Failed to retrieve object");
            Err(e)
            // Ok(HttpResponse::NotFound().json(ObjectNotFoundResponse {
            //     name: object_key,
            //     bucket: bucket_name,
            //     message: "Object not found".to_string(),
            // }))
        }
    }
}

/// Handles DELETE /buckets/{bucket_name}/objects/{object_key}
/// Deletes an object from a bucket.
#[tracing::instrument(
    name = "Delete object",
    skip(s3_service),
    fields(
        bucket = %path.0,
        object_key = %path.1
    )
)]
async fn delete_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let mut s3 = s3_service.lock().unwrap();
    match s3.delete_object(&bucket_name, &object_key) {
        Ok(_) => {
            info!(
                "Object '{}' deleted from bucket '{}'.",
                object_key, bucket_name
            );
            Ok(HttpResponse::NoContent().json(ObjectDeletedResponse {
                name: object_key,
                bucket: bucket_name,
                message: "Object deleted successfully".to_string(),
            }))
        }
        Err(e) => {
            error!(error = %e, "Failed to delete object");
            Err(e)
        }
    }
}

/// Handles GET /buckets/{bucket_name}/objects
/// Lists all objects in a specific bucket.
async fn list_objects_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let s3 = s3_service.lock().unwrap();
    match s3.list_objects(&bucket_name) {
        Ok(objects) => {
            info!(
                "Listed {} objects in bucket '{}'.",
                objects.len(),
                bucket_name
            );
            Ok(HttpResponse::Ok().json(ObjectListResponse {
                bucket: bucket_name,
                items: objects,
            }))
        }
        Err(e) => {
            error!(error = %e, "Failed to list objects");
            // Err(e)
            Ok(HttpResponse::NotFound().json(BucketNotFoundResponse {
                bucket: bucket_name,
                message: "Bucket not found".to_string(),
            }))
        }
    }
}

// The main function is now asynchronous and sets up the Actix Web server.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    init_logging();

    info!("Starting S3-like Storage HTTP API on http://127.0.0.1:8080");

    // Initialize Storage first, as S3Service will depend on it for bucket creation
    let db_path = "s3_storage.db"; // Use a specific file for the SQLite DB
    let storage = match Storage::new(db_path) {
        Ok(s) => Arc::new(Mutex::new(s)),
        Err(e) => {
            error!("Failed to initialize storage: {}", e);
            // Convert the StorageError to an IoError to fit the main function's return type
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Storage initialization failed: {}", e)));
        }
    };
    
    // Initialize S3Service
    let s3_service = Arc::new(Mutex::new(S3Service::new()));

    HttpServer::new(move || {
        // Create a web::Data instance for storage
        let storage_data = web::Data::new(storage.clone());
        let s3_service_data = web::Data::new(s3_service.clone());
        
        App::new()
            // Add tracing middleware for request logging
            .wrap(TracingLogger::default())
            // Register application data
            .app_data(s3_service_data.clone()) // Ensure clones are used for each .app_data call
            .app_data(storage_data.clone()) 
            .service(
                web::resource("/buckets/{bucket_name}")
                    .put(create_bucket_handler) // PUT to create bucket
                    .delete(delete_bucket_handler), // DELETE to delete bucket
            )
            .service(
                web::resource("/buckets").get(list_buckets_handler), // GET to list all buckets
            )
            .service(
                web::resource("/buckets/{bucket_name}/objects/{object_key}")
                    .put(put_object_handler) // PUT to put object
                    .get(get_object_handler) // GET to get object
                    .delete(delete_object_handler), // DELETE to delete object
            )
            .service(
                web::resource("/buckets/{bucket_name}/objects").get(list_objects_handler), // GET to list objects in a bucket
            )
            .default_service(web::to(|| async { HttpResponse::NotFound().finish() }))
    })
    .bind(("127.0.0.1", 8080))?
    .workers(5)
    .run()
    .await
}