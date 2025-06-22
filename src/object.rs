// object.rs
// This module defines the Object structure, representing a stored item within a bucket.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTimeError;
use thiserror::Error;

/// Represents an object stored within an S3-like bucket.
/// It contains the object's key (its unique identifier within the bucket)
/// and the actual binary data.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(dead_code)]
pub struct Object {
    pub key: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub data: Vec<u8>, // Stored as raw bytes
    // In a real S3, you might also have metadata like:
    pub content_type: Option<String>,
    #[serde(skip_serializing)]
    pub etag: Option<String>, // Hash of the object's data
    pub last_modified: i64,
    #[serde(skip_serializing)]
    pub user_metadata: Option<HashMap<String, String>>,
}

/// Custom error type for operations within the object module.
#[derive(Debug, Error, Serialize)]
pub enum ObjectError {
    #[error("Failed to get system time: {0}")]
    #[serde(skip_serializing)]
    SystemTime(#[from] SystemTimeError),
}

impl Object {
    /// Creates a new Object instance.
    ///
    /// # Arguments
    ///
    /// * `key` - The unique identifier for the object within its bucket.
    /// * `data` - The binary data of the object.
    /// * `content_type` - The MIME type of the object.
    /// * `user_metadata` - Optional user metadata for the object.
    ///
    /// # Returns
    ///
    /// * `Object` - The newly created Object instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::object::Object;
    /// let object = Object::new("my-object-key".to_string(), vec![1, 2, 3], None, None);
    /// ```
    pub fn new(
        key: String,
        data: Vec<u8>,
        content_type: Option<String>,
        user_metadata: Option<HashMap<String, String>>,
    ) -> Result<Self, ObjectError> {
        let last_modified = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)? // Use '?' to propagate the error
            .as_secs() as i64;
        Ok(Object {
            key,
            data,
            content_type,
            etag: None,
            last_modified,
            user_metadata,
        })
    }

    /// Returns the size of the object data in bytes.
    ///
    /// # Returns
    ///
    /// * `usize` - The size of the object data in bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::object::Object;
    /// let object = Object::new("my-object-key".to_string(), vec![1, 2, 3], None, None);
    /// assert_eq!(object.unwrap().size(), 3);
    /// ```
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}
