// object.rs
// This module defines the Object structure, representing a stored item within a bucket.

use std::collections::HashMap;
use md5::Md5;
use hex;
use md5::Digest;
use serde::Serialize;

/// Represents an object stored within an S3-like bucket.
/// It contains the object's key (its unique identifier within the bucket)
/// and the actual binary data.
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct Object {
    pub key: String,
    #[serde(skip_serializing)]
    pub data: Vec<u8>, // Stored as raw bytes
    // In a real S3, you might also have metadata like:
    pub content_type: Option<String>,
    #[serde(skip_serializing)]
    pub etag: String, // Hash of the object's data
    pub last_modified: std::time::SystemTime,
    #[serde(skip_serializing)]
    pub user_metadata: HashMap<String, String>,
}

fn calculate_etag(data: &Vec<u8>) -> String {
    let mut hasher = Md5::default();
    hasher.input(data);
    hex::encode(hasher.result())
}

impl Object {
    /// Creates a new Object instance.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The unique identifier for the object within its bucket.
    /// * `data` - The binary data of the object.
    /// 
    /// # Returns
    /// 
    /// * `Object` - The newly created Object instance.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use s3_learning_project::object::Object;
    /// let object = Object::new("my-object-key".to_string(), vec![1, 2, 3]);
    /// ```
    pub fn new(key: String, data: Vec<u8>, content_type: Option<String>, user_metadata: HashMap<String, String>) -> Self {
        let etag = calculate_etag(&data);
        Object {
            key,
            data,
            content_type,
            etag,
            last_modified: std::time::SystemTime::now(),
            user_metadata,
        }
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
    /// let object = Object::new("my-object-key".to_string(), vec![1, 2, 3]);
    /// assert_eq!(object.size(), 3);
    /// ```
    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}