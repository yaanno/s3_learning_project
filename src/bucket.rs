// bucket.rs
// This module defines the Bucket structure, representing an S3 bucket.
// Each bucket manages its own collection of objects by interacting with the Storage layer.

use crate::object::{Object, ObjectError}; // Import Object and ObjectError
use crate::Storage; // This line should work!
use std::sync::{Arc, Mutex};
use thiserror::Error; // Add this for potential BucketError later, or for StorageError::from
/// Custom error type for operations within the bucket module.
#[derive(Debug, Error)]
pub enum BucketError {
    #[error("Object creation failed: {0}")]
    ObjectCreationError(#[from] ObjectError), // If you create objects inside Bucket methods
    #[error("Failed to acquire storage lock")]
    LockAcquisitionFailed, // For the unwrap() on mutex.lock()
    #[error("Storage error: {0}")]
    StorageError(String),
}


/// Represents an S3-like bucket.
/// It holds a reference to the shared Storage instance.
#[allow(dead_code)]
pub struct Bucket {
    name: String,
    storage: Arc<Mutex<Storage>>,
}

impl Bucket {
    /// Creates a new Bucket instance with the given name and a reference to the shared Storage.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket.
    /// * `storage` - An Arc<Mutex> reference to the shared Storage instance.
    ///
    /// # Returns
    ///
    /// * `Bucket` - The newly created Bucket instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::storage::Storage;
    /// use std::sync::{Arc, Mutex};
    /// // In a real scenario, db_path would be passed or configured
    /// let storage = Arc::new(Mutex::new(Storage::new(":memory:").unwrap()));
    /// let bucket = Bucket::new("my-bucket".to_string(), storage.clone());
    /// ```
    #[allow(dead_code)]
    pub fn new(name: String, storage: Arc<Mutex<Storage>>) -> Self {
        Bucket { name, storage }
    }

    /// Returns the name of the bucket.
    ///
    /// # Returns
    ///
    /// * `&str` - The name of the bucket.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::storage::Storage;
    /// use std::sync::{Arc, Mutex};
    /// let storage = Arc::new(Mutex::new(Storage::new(":memory:").unwrap()));
    /// let bucket = Bucket::new("my-bucket".to_string(), storage.clone());
    /// assert_eq!(bucket.get_name(), "my-bucket");
    /// ```
    #[allow(dead_code)]
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Stores an object in the bucket via the Storage layer.
    /// If an object with the same key already exists, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `key` - The unique identifier for the object within its bucket.
    /// * `data` - The binary data of the object.
    /// * `content_type` - Optional content type for the object.
    /// * `user_metadata` - Optional user-defined metadata for the object.
    ///
    /// # Returns
    ///
    /// * `Result<(), BucketError>` - Ok(()) on success, or a BucketError on failure.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::storage::Storage;
    /// use std::sync::{Arc, Mutex};
    /// let storage = Arc::new(Mutex::new(Storage::new(":memory:").unwrap()));
    /// let bucket = Bucket::new("my-bucket".to_string(), storage.clone());
    /// // Assuming put_object is called on the bucket directly, not on a full Object
    /// bucket.put_object("my-object-key", &[1, 2, 3], None, None).unwrap();
    /// ```
    pub fn put_object(&self, object: Object) -> Result<(), BucketError> {
        let mut storage_guard = self
            .storage
            .lock()
            .map_err(|_| BucketError::LockAcquisitionFailed)?;
        let _ = storage_guard.put_object(&self.name, object).map_err(|e| BucketError::StorageError(e.to_string()));
        Ok(())
    }

    /// Retrieves an object from the bucket by its key via the Storage layer.
    /// Returns `Object` if found, or a `BucketError` otherwise.
    ///
    /// # Arguments
    ///
    /// * `key` - The unique identifier for the object within its bucket.
    ///
    /// # Returns
    ///
    /// * `Result<Object, BucketError>` - The retrieved Object on success, or a BucketError.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::storage::Storage;
    /// use std::sync::{Arc, Mutex};
    /// let storage = Arc::new(Mutex::new(Storage::new(":memory:").unwrap()));
    /// let bucket = Bucket::new("my-bucket".to_string(), storage.clone());
    /// bucket.put_object("my-object-key", &[1, 2, 3], None, None).unwrap();
    /// let retrieved_object = bucket.get_object("my-object-key").unwrap();
    /// assert_eq!(retrieved_object.data, vec![1, 2, 3]);
    /// ```
    pub fn get_object(&self, key: &str) -> Result<Object, BucketError> {
        let storage_guard = self
            .storage
            .lock()
            .map_err(|_| BucketError::LockAcquisitionFailed)?;
        // storage_guard.get_object(&self.name, key).map_err(Into::into)
        let object = storage_guard.get_object(&self.name, key);
        match object {
            Ok(object) => Ok(object),
            Err(e) => Err(BucketError::StorageError(e.to_string())),
        }
    }

    /// Deletes an object from the bucket by its key via the Storage layer.
    /// Returns `true` if the object was found and removed, `false` otherwise (or error).
    pub fn delete_object(&self, key: &str) -> Result<bool, BucketError> {
        let mut storage_guard = self
            .storage
            .lock()
            .map_err(|_| BucketError::LockAcquisitionFailed)?;
        match storage_guard.delete_object(&self.name, key) {
            Ok(result) => Ok(result),
            Err(e) => Err(BucketError::StorageError(e.to_string())),
        }
    }

    /// Lists the keys of all objects currently stored in the bucket via the Storage layer.
    pub fn list_objects(&self) -> Result<Vec<String>, BucketError> {
        let storage_guard = self
            .storage
            .lock()
            .map_err(|_| BucketError::LockAcquisitionFailed)?;
        match storage_guard.list_objects(&self.name) {
            Ok(result) => Ok(result),
            Err(e) => Err(BucketError::StorageError(e.to_string())),
        }
    }

    /// Checks if the bucket is empty (contains no objects) via the Storage layer.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the bucket is empty, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::storage::Storage;
    /// use std::sync::{Arc, Mutex};
    /// let storage = Arc::new(Mutex::new(Storage::new(":memory:").unwrap()));
    /// let bucket = Bucket::new("my-bucket".to_string(), storage.clone());
    /// assert!(bucket.is_empty().unwrap());
    /// bucket.put_object("test", &[1], None, None).unwrap();
    /// assert!(!bucket.is_empty().unwrap());
    /// ```
    #[allow(dead_code)]
    pub fn is_empty(&self) -> Result<bool, BucketError> {
        let storage_guard = self
            .storage
            .lock()
            .map_err(|_| BucketError::LockAcquisitionFailed)?;
        storage_guard
            .is_empty(&self.name)
            .map_err(|e| BucketError::StorageError(e.to_string()))
    }
}
