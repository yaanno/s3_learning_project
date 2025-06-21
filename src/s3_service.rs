// s3_service.rs
// (This is the expected content based on your main.rs and the fix)

use crate::bucket::{Bucket, BucketError}; // Ensure Bucket and BucketError are imported
use crate::object::{Object, ObjectError}; // Ensure Object and ObjectError are imported
use crate::storage::{Storage}; // Ensure Storage and StorageError are imported
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use serde::Serialize;

/// Represents custom errors that can occur in our S3-like service.
#[derive(Debug, Error, Serialize)]
pub enum S3Error {
    #[error("Bucket '{0}' already exists")]
    BucketAlreadyExists(String),
    #[error("Bucket '{0}' not found")]
    BucketNotFound(String),
    #[error("Object '{0}' not found in bucket '{1}'")]
    ObjectNotFound(String, String),
    #[error("Object creation failed: {0}")]
    ObjectCreationFailed(#[from] ObjectError),
    #[error("Bucket operation failed: {0}")] // This variant needs to be present
    BucketOperationFailed(#[from] BucketError),
}

/// The main S3-like service structure.
pub struct S3Service {
    buckets: HashMap<String, Bucket>,
}

impl S3Service {
    /// Creates a new, empty S3Service instance.
    pub fn new() -> Self {
        S3Service {
            buckets: HashMap::new(),
        }
    }

    /// Creates a new bucket with the given name.
    /// It now takes the `Arc<Mutex<Storage>>` to pass to the `Bucket` constructor.
    pub fn create_bucket(&mut self, name: &str, storage: Arc<Mutex<Storage>>) -> Result<(), S3Error> {
        if self.buckets.contains_key(name) {
            return Err(S3Error::BucketAlreadyExists(name.to_string()));
        }
        // Pass the cloned Arc<Mutex<Storage>> to the new Bucket
        let new_bucket = Bucket::new(name.to_string(), storage.clone());
        self.buckets.insert(name.to_string(), new_bucket);
        Ok(())
    }

    pub fn delete_bucket(&mut self, name: &str) -> Result<(), S3Error> {
        if self.buckets.remove(name).is_some() {
            Ok(())
        } else {
            Err(S3Error::BucketNotFound(name.to_string()))
        }
    }

    pub fn list_buckets(&self) -> Vec<String> {
        self.buckets.keys().cloned().collect()
    }

    // --- Object Operations (Delegating to Bucket) ---

    // Note: The `put_object` signature in main.rs suggests `S3Service::put_object`
    // returns `Result<&Object, S3Error>`. This implies the Object is stored *inside*
    // the Bucket and a reference to it is returned. This can be tricky with `Object` being
    // a struct that contains `Vec<u8>`. Returning a reference from a `MutexGuard` is possible,
    // but often it's simpler to return an owned `Object` or a simplified `ObjectInfo` struct
    // after the operation. I'll make an assumption here that `bucket.get_object`
    // returns an owned `Object` and then `S3Service::put_object` also returns an owned `Object` (or clone).
    // If you explicitly need `&Object`, you'd need to manage lifetimes from the MutexGuard more carefully.

    pub fn put_object(
        &mut self,
        bucket_name: &str,
        object: Object, // Takes the full Object as input
    ) -> Result<Object, S3Error> { // Changed return type to owned Object for simplicity
        let bucket = self.buckets.get_mut(bucket_name)
            .ok_or_else(|| S3Error::BucketNotFound(bucket_name.to_string()))?;

        // Delegate to the bucket's put_object
        bucket.put_object(object.clone())
              .map_err(S3Error::BucketOperationFailed)?; // Convert BucketError to S3Error

        // After putting, retrieve the object from the bucket to return it
        bucket.get_object(&object.key).map_err(S3Error::BucketOperationFailed)
    }

    pub fn get_object(&self, bucket_name: &str, key: &str) -> Result<Object, S3Error> {
        let bucket = self.buckets.get(bucket_name)
            .ok_or_else(|| S3Error::BucketNotFound(bucket_name.to_string()))?;
        bucket.get_object(key).map_err(S3Error::BucketOperationFailed)
    }

    pub fn delete_object(&mut self, bucket_name: &str, key: &str) -> Result<(), S3Error> {
        let bucket = self.buckets.get_mut(bucket_name)
            .ok_or_else(|| S3Error::BucketNotFound(bucket_name.to_string()))?;
        match bucket.delete_object(key) {
            Ok(true) => Ok(()), // Object was found and deleted
            Ok(false) => Err(S3Error::ObjectNotFound(key.to_string(), bucket_name.to_string())),
            Err(e) => Err(S3Error::BucketOperationFailed(e)),
        }
    }

    pub fn list_objects(&self, bucket_name: &str) -> Result<Vec<String>, S3Error> {
        let bucket = self.buckets.get(bucket_name)
            .ok_or_else(|| S3Error::BucketNotFound(bucket_name.to_string()))?;
        bucket.list_objects().map_err(S3Error::BucketOperationFailed)
    }
}