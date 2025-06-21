use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::http::header::CONTENT_TYPE;
use actix_web::web;
use actix_web::web::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use crate::S3Error;
use crate::S3Service;
use crate::object::Object;
use crate::structs::{
    BucketCreatedResponse, BucketDeletedResponse, ListResponse, ObjectCreatedResponse,
    ObjectDeletedResponse, ObjectListResponse,
};

/// Handles GET /buckets/{bucket_name}/objects/{object_key}
/// Retrieves an object from a bucket.
///
/// # Arguments
///
/// * `s3_service` - A reference to the S3Service instance.
/// * `path` - The path to the object to retrieve.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
#[tracing::instrument(
    name = "Get object",
    skip(s3_service),
    fields(
        bucket = %path.0,
        object_key = %path.1
    )
)]
pub async fn get_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let s3 = s3_service
        .lock()
        .expect("Acquire lock on S3 service failed");
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
            Ok(response.body(object.data))
        }
        Err(e) => {
            error!(error = %e, "Failed to retrieve object");
            Err(e)
        }
    }
}

/// Handles PUT /buckets/{bucket_name}
/// Creates a new bucket.
///
/// # Arguments
///
/// * `s3_service` - A reference to the S3Service instance.
/// * `path` - The path to the bucket to create.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
pub async fn create_bucket_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    // storage: web::Data<Arc<Mutex<Storage>>>, // REMOVE THIS ARGUMENT - S3Service now manages Storage
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let mut s3 = s3_service
        .lock()
        .expect("Acquire lock on S3 service failed");
    // Call create_bucket without the storage argument
    match s3.create_bucket(&bucket_name) {
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
///
/// # Arguments
///
/// * `s3_service` - A reference to the S3Service instance.
/// * `path` - The path to the bucket to delete.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
pub async fn delete_bucket_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let mut s3 = s3_service
        .lock()
        .expect("Acquire lock on S3 service failed");
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
///
/// # Arguments
///
/// * `s3_service` - A reference to the S3Service instance.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
pub async fn list_buckets_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
) -> Result<HttpResponse, S3Error> {
    let s3 = s3_service
        .lock()
        .expect("Acquire lock on S3 service failed");
    let buckets = s3.list_buckets();
    Ok(HttpResponse::Ok().json(ListResponse { items: buckets }))
}

/// Handles PUT /buckets/{bucket_name}/objects/{object_key}
/// Puts an object into a bucket. The object data is taken from the request body.
///
/// # Arguments
///
/// * `req` - The HTTP request.
/// * `s3_service` - A reference to the S3Service instance.
/// * `path` - The path to the object to put.
/// * `body` - The body of the request.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
#[tracing::instrument(
    name = "Put object",
    skip(s3_service, body, req),
    fields(
        bucket = %path.0,
        object_key = %path.1,
        object_size = body.len()
    )
)]
pub async fn put_object_handler(
    req: HttpRequest,
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
    body: Bytes, // Raw bytes from the request body
) -> Result<HttpResponse, S3Error> {
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let user_metadata = req
        .headers()
        .iter()
        .filter(|(k, _)| k.as_str().starts_with("x-user-meta-"))
        .filter_map(|(k, v)| {
            v.to_str().ok().map(|val_str| {
                (
                    k.as_str()
                        .strip_prefix("x-user-meta-")
                        .unwrap_or(k.as_str())
                        .to_string(),
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

    // Create the Object before acquiring the lock
    let object = Object::new(
        object_key.clone(),
        body.to_vec(),
        content_type,
        Some(user_metadata),
    )?;

    // Acquire the lock, call put_object, and release the lock immediately
    let result = {
        let mut s3 = s3_service.lock().map_err(|_| {
            S3Error::InternalStorageError("Failed to acquire lock on S3 service".to_string())
        })?;
        s3.put_object(&bucket_name, object)
    };

    match result {
        Ok(returned_object) => {
            info!(
                "Object '{}' put into bucket '{}'.",
                returned_object.key, bucket_name
            );
            Ok(HttpResponse::Created().json(ObjectCreatedResponse {
                name: returned_object.key.clone(),
                bucket: bucket_name,
                metadata: &returned_object,
                message: "Object created successfully".to_string(),
            }))
        }
        Err(e) => {
            error!(error = %e, "Failed to store object");
            Err(e)
        }
    }
}

/// Handles DELETE /buckets/{bucket_name}/objects/{object_key}
/// Deletes an object from a bucket.
///
/// # Arguments
///
/// * `s3_service` - A reference to the S3Service instance.
/// * `path` - The path to the object to delete.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
#[tracing::instrument(
    name = "Delete object",
    skip(s3_service),
    fields(
        bucket = %path.0,
        object_key = %path.1
    )
)]
pub async fn delete_object_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<(String, String)>,
) -> Result<HttpResponse, S3Error> {
    let (bucket_name, object_key) = path.into_inner();
    let mut s3 = s3_service
        .lock()
        .expect("Acquire lock on S3 service failed");
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
///
/// # Arguments
///
/// * `s3_service` - A reference to the S3Service instance.
/// * `path` - The path to the bucket to list objects from.
///
/// # Returns
///
/// * `Result<HttpResponse, S3Error>` - The HTTP response, or an error.
pub async fn list_objects_handler(
    s3_service: web::Data<Arc<Mutex<S3Service>>>,
    path: web::Path<String>,
) -> Result<HttpResponse, S3Error> {
    let bucket_name = path.into_inner();
    let s3 = s3_service
        .lock()
        .expect("Acquire lock on S3 service failed");
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
            Err(e)
        }
    }
}
