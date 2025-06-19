// main.rs
// This file now sets up an HTTP server to expose the S3-like service.

mod s3_service; // Declare the s3_service module
mod bucket;     // Declare the bucket module
mod object;     // Declare the object module

use actix_web::{
    web, App, HttpServer, HttpResponse, error::ResponseError, web::Bytes
};

use actix_web::http::StatusCode;
use s3_service::{S3Service, S3Error};
use std::sync::{Arc, Mutex};
use serde::Serialize; // For JSON responses

// --- Helper function to map S3Error to Actix Web HTTP responses ---
impl ResponseError for S3Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .insert_header(actix_web::http::header::ContentType::plaintext())
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            S3Error::BucketAlreadyExists => StatusCode::CONFLICT, // 409 Conflict
            S3Error::BucketNotFound => StatusCode::NOT_FOUND,     // 404 Not Found
            S3Error::ObjectNotFound => StatusCode::NOT_FOUND,     // 404 Not Found
            S3Error::InvalidOperation(_) => StatusCode::BAD_REQUEST, // 400 Bad Request
        }
    }
}

// --- Request/Response Structs (for JSON where applicable) ---

// For listing buckets or objects
#[derive(Serialize)]
struct ListResponse {
    items: Vec<String>,
}

// --- Handler Functions for API Endpoints ---

/// Handles PUT /buckets/{bucket_name}
/// Creates a new bucket.
async fn create_bucket_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let mut s3 = s3_service.lock().unwrap(); // Acquire mutex lock
    s3.create_bucket(&bucket_name)?; // The '?' operator propagates S3Error
    Ok(HttpResponse::Created().body(format!("Bucket '{}' created.", bucket_name)))
}

/// Handles DELETE /buckets/{bucket_name}
/// Deletes an existing bucket.
async fn delete_bucket_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let mut s3 = s3_service.lock().unwrap();
    s3.delete_bucket(&bucket_name)?;
    Ok(HttpResponse::NoContent().body(format!("Bucket '{}' deleted.", bucket_name)))
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
async fn put_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
    body: Bytes, // Raw bytes from the request body
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let mut s3 = s3_service.lock().unwrap();
    // Convert Bytes to Vec<u8> for storage
    s3.put_object(&bucket_name, &object_key, body.to_vec())?;
    Ok(HttpResponse::Ok().body(format!("Object '{}' put into bucket '{}'.", object_key, bucket_name)))
}

/// Handles GET /buckets/{bucket_name}/objects/{object_key}
/// Retrieves an object from a bucket.
async fn get_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let s3 = s3_service.lock().unwrap();
    let object = s3.get_object(&bucket_name, &object_key)?; // Get object reference
    Ok(HttpResponse::Ok().body(object.data.clone())) // Clone data to return owned Vec<u8>
}

/// Handles DELETE /buckets/{bucket_name}/objects/{object_key}
/// Deletes an object from a bucket.
async fn delete_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let mut s3 = s3_service.lock().unwrap();
    s3.delete_object(&bucket_name, &object_key)?;
    Ok(HttpResponse::NoContent().body(format!("Object '{}' deleted from bucket '{}'.", object_key, bucket_name)))
}

/// Handles GET /buckets/{bucket_name}/objects
/// Lists all objects in a specific bucket.
async fn list_objects_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let s3 = s3_service.lock().unwrap();
    let objects = s3.list_objects(&bucket_name)?;
    Ok(HttpResponse::Ok().json(ListResponse { items: objects }))
}

// The main function is now asynchronous and sets up the Actix Web server.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting S3-like Storage HTTP API on http://127.0.0.1:8080");

    // Create a shared S3Service instance wrapped in Arc (Atomic Reference Counting)
    // and Mutex (for mutual exclusion, ensuring thread-safe access).
    let s3_data = web::Data::new(Arc::new(Mutex::new(S3Service::new())));

    HttpServer::new(move || {
        App::new()
            .app_data(s3_data.clone()) // Pass the shared S3Service data to the app
            .service(
                web::resource("/buckets/{bucket_name}")
                    .put(create_bucket_handler)    // PUT to create bucket
                    .delete(delete_bucket_handler), // DELETE to delete bucket
            )
            .service(
                web::resource("/buckets")
                    .get(list_buckets_handler),    // GET to list all buckets
            )
            .service(
                web::resource("/buckets/{bucket_name}/objects/{object_key}")
                    .put(put_object_handler)       // PUT to put object
                    .get(get_object_handler)       // GET to get object
                    .delete(delete_object_handler), // DELETE to delete object
            )
            .service(
                web::resource("/buckets/{bucket_name}/objects")
                    .get(list_objects_handler),    // GET to list objects in a bucket
            )
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
