// s3_service.rs
use crate::bucket::{Bucket, BucketError};
use crate::object::{Object, ObjectError};
use crate::storage::{Storage, StorageError};
use std::sync::{Arc};
use thiserror::Error;
use tokio::sync::Mutex;

/// Represents custom errors that can occur in our S3-like service.
#[derive(Debug, Error)]
pub enum S3Error {
    #[error("Bucket '{0}' already exists")]
    BucketAlreadyExists(String),
    #[error("Bucket '{0}' not found")]
    BucketNotFound(String),
    #[error("Object '{0}' not found in bucket '{1}'")]
    ObjectNotFound(String, String),
    #[error("Object creation failed: {0}")]
    ObjectCreationFailed(#[from] ObjectError),
    #[error("Bucket operation failed: {0}")]
    BucketOperationFailed(#[from] BucketError),
    #[error("Internal storage error: {0}")]
    InternalStorageError(String),
}

pub struct S3Service {
    storage: Arc<Mutex<Storage>>,
}

impl S3Service {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        S3Service { storage }
    }

    /// Creates a new bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to create.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - An empty result, or an error.
    pub async fn create_bucket(&mut self, name: &str) -> Result<(), S3Error> {
        let result = {
            let mut lock = self.storage.lock().await;
            lock.create_bucket(name)
        };

        match result {
            Ok(_) => Ok(()),
            Err(StorageError::BucketAlreadyExistsInStorage(bucket_name)) => {
                Err(S3Error::BucketAlreadyExists(bucket_name))
            }
            Err(e) => Err(S3Error::InternalStorageError(format!(
                "Failed to create bucket in storage: {}",
                e
            ))),
        }
    }

    /// Deletes a bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to delete.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - An empty result, or an error.
    pub async fn delete_bucket(&mut self, name: &str) -> Result<(), S3Error> {
        let result = {
            let mut lock = self.storage.lock().await;
            lock._delete_bucket(name)
        };

        match result {
            Ok(_) => Ok(()),
            Err(StorageError::BucketNotFoundInStorage(bucket_name)) => {
                Err(S3Error::BucketNotFound(bucket_name))
            }
            Err(e) => Err(S3Error::InternalStorageError(format!(
                "Failed to delete bucket from storage: {}",
                e
            ))),
        }
    }

    /// Lists all buckets.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - A vector of bucket names.
    pub async fn list_buckets(&self) -> Result<Vec<String>, S3Error> {
        let result = {
            let storage_lock = self.storage.lock().await;
            storage_lock.list_buckets()
        };
        match result {
            Ok(buckets) => Ok(buckets),
            Err(e) => {
                eprintln!("Error listing buckets from storage: {}", e);
                Err(S3Error::InternalStorageError(format!(
                    "Failed to list buckets from storage: {}",
                    e
                )))
            }
        }
    }

    /// Helper to get a Bucket instance on demand
    async fn get_bucket_instance(&self, bucket_name: &str) -> Result<Bucket, S3Error> {
        let result = {
            let storage_lock = self.storage.lock().await;
            storage_lock.bucket_exists(bucket_name)
        };
        match result {
            Ok(true) => Ok(Bucket::new(bucket_name.to_string(), self.storage.clone())),
            Ok(false) => Err(S3Error::BucketNotFound(bucket_name.to_string())),
            Err(e) => Err(S3Error::InternalStorageError(format!(
                "Error checking bucket existence: {}",
                e
            ))),
        }
    }

    /// Puts an object into a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket to put the object into.
    /// * `object` - The object to put into the bucket.
    ///
    /// # Returns
    ///
    /// * `Result<Object, S3Error>` - The put object, or an error.
    pub async fn put_object(&mut self, bucket_name: &str, object: Object) -> Result<Object, S3Error> {
        let mut bucket = self.get_bucket_instance(bucket_name).await?;
        let result = bucket.put_object(
            &object.key,
            &object.data,
            object.content_type.as_deref(),
            object.user_metadata.as_ref(),
        );
        match result.await {
            Ok(object) => Ok(object),
            Err(e) => Err(S3Error::BucketOperationFailed(e)),
        }
    }

    /// Retrieves an object from a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket to retrieve the object from.
    /// * `key` - The key of the object to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<Object, S3Error>` - The retrieved object, or an error.
    pub async fn get_object(&self, bucket_name: &str, key: &str) -> Result<Object, S3Error> {
        let bucket = self.get_bucket_instance(bucket_name).await?;
        match bucket.get_object(key).await {
            Ok(object) => Ok(object),
            Err(e) => Err(S3Error::BucketOperationFailed(e)),
        }
    }

    /// Deletes an object from a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket to delete the object from.
    /// * `key` - The key of the object to delete.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - An empty result, or an error.
    pub async fn delete_object(&mut self, bucket_name: &str, key: &str) -> Result<(), S3Error> {
        let mut bucket = self.get_bucket_instance(bucket_name).await?;
        match bucket.delete_object(key).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(S3Error::ObjectNotFound(
                key.to_string(),
                bucket_name.to_string(),
            )),
            Err(e) => Err(S3Error::BucketOperationFailed(e)),
        }
    }

    /// Lists all objects in a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket to list objects from.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<String>, S3Error>` - A vector of object keys in the bucket, or an error.
    pub async fn list_objects(&self, bucket_name: &str) -> Result<Vec<String>, S3Error> {
        let bucket = self.get_bucket_instance(bucket_name).await?;
        let result = bucket.list_objects().await;
        match result {
            Ok(objects) => Ok(objects),
            Err(e) => Err(S3Error::BucketOperationFailed(e)),
        }
    }
}
