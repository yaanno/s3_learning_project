// bucket.rs
use crate::object::{Object, ObjectError}; // Ensure Object and ObjectError are accessible
use crate::storage::{Storage, StorageError}; // Import Storage and StorageError
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BucketError {
    #[error("Object '{0}' not found in bucket")]
    ObjectNotFound(String),
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
    // `Bucket::new` now takes the Arc<Mutex<Storage>>
    pub fn new(name: String, storage: Arc<Mutex<Storage>>) -> Self {
        Bucket {
            name,
            storage, // Store the clone of the Arc
        }
    }

    pub fn put_object(
        &mut self,
        key: &str,
        data: &[u8],
        content_type: Option<&str>,
        user_metadata: Option<&HashMap<String, String>>,
    ) -> Result<Object, BucketError> {
        // Return the created Object (from get_object)
        // First, create the Object struct. This part is in-memory.
        let object_to_store = Object::new(
            key.to_string(),
            data.to_vec(),
            content_type.map(|s| s.to_string()),
            user_metadata.cloned(),
        )?; // Converts ObjectError into BucketError::ObjectDataError

        let mut storage_lock = self.storage.lock().unwrap();
        // Delegate the actual storage persistence to the Storage module
        storage_lock.put_object(&self.name, object_to_store)?; // Converts StorageError into BucketError::Storage

        // Retrieve the object after putting it, to ensure we return the persisted state
        // (e.g., with updated etag or last_modified from storage logic)
        self.get_object(key) // This handles the ObjectNotFound and Storage errors
    }

    pub fn get_object(&self, key: &str) -> Result<Object, BucketError> {
        let storage_lock = self.storage.lock().unwrap();
        storage_lock
            .get_object(&self.name, key)
            .map_err(|e| match e {
                StorageError::ObjectNotFound(_, _) => BucketError::ObjectNotFound(key.to_string()),
                _ => BucketError::Storage(e), // Convert other StorageError types
            })
    }

    pub fn delete_object(&mut self, key: &str) -> Result<bool, BucketError> {
        let mut storage_lock = self.storage.lock().unwrap();
        storage_lock
            .delete_object(&self.name, key)
            .map_err(BucketError::Storage) // Convert any StorageError
    }

    pub fn list_objects(&self) -> Result<Vec<String>, BucketError> {
        let storage_lock = self.storage.lock().unwrap();
        storage_lock
            .list_objects(&self.name)
            .map_err(BucketError::Storage)
    }
}
