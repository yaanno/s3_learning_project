// bucket.rs
use crate::object::{Object, ObjectError}; // Ensure Object and ObjectError are accessible
use crate::storage::{Storage, StorageError}; // Import Storage and StorageError
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Debug, Error)]
pub enum BucketError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError), // Allow converting StorageError to BucketError
    #[error("Invalid object data: {0}")] // Added for Object creation errors
    ObjectDataError(#[from] ObjectError),
}

pub struct Bucket {
    pub name: String,
    // The bucket no longer holds objects directly in a HashMap.
    // Instead, it holds a reference to the shared Storage.
    storage: Arc<Mutex<Storage>>,
}

impl Bucket {
    /// Creates a new bucket.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to create.
    /// * `storage` - The storage to use for the bucket.
    ///
    /// # Returns
    ///
    /// * `Bucket` - The created bucket.
    pub fn new(name: String, storage: Arc<Mutex<Storage>>) -> Self {
        Bucket {
            name,
            storage, // Store the clone of the Arc
        }
    }

    /// Puts an object into the bucket.
    ///
    /// # Arguments
    ///
    /// * `object` - The object to put into the bucket.
    ///
    /// # Returns
    ///
    /// * `Result<Object, BucketError>` - The object that was put, or an error.
    pub async fn put_object(&mut self, object: Object) -> Result<Object, BucketError> {
        // Return the created Object (from get_object)
        // First, create the Object struct. This part is in-memory.
        let result = {
            let mut storage_lock = self.storage.lock().await;
            storage_lock.put_object(&self.name, object.clone())
        };

        match result {
            Ok(_) => {
                if let Ok(object) = self.get_object(&object.key).await {
                    Ok(object)
                } else {
                    Err(BucketError::Storage(StorageError::ObjectNotFound(
                        object.key.clone(),
                        self.name.clone(),
                    )))
                }
            }
            Err(e) => Err(BucketError::Storage(e)),
        }
    }

    /// Gets an object from the bucket.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the object to get.
    ///
    /// # Returns
    ///
    /// * `Result<Object, BucketError>` - The object that was retrieved, or an error.
    pub async fn get_object(&self, key: &str) -> Result<Object, BucketError> {
        let object = {
            let lock = self.storage.lock().await;
            lock.get_object(&self.name, key)
        };
        Ok(object?)
    }

    /// Deletes an object from the bucket.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the object to delete.
    ///
    /// # Returns
    ///
    /// * `Result<bool, BucketError>` - Whether the object was deleted, or an error.
    pub async fn delete_object(&mut self, key: &str) -> Result<bool, BucketError> {
        let object = {
            let mut lock = self.storage.lock().await;
            lock.delete_object(&self.name, key)
        };
        Ok(object?)
    }

    /// Lists all objects in the bucket.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<String>, BucketError>` - A vector of object keys in the bucket, or an error.
    pub async fn list_objects(&self) -> Result<Vec<String>, BucketError> {
        let object = {
            let lock = self.storage.lock().await;
            lock.list_objects(&self.name)
        };
        Ok(object?)
    }
}
