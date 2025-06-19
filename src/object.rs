// object.rs
// This module defines the Object structure, representing a stored item within a bucket.

/// Represents an object stored within an S3-like bucket.
/// It contains the object's key (its unique identifier within the bucket)
/// and the actual binary data.
#[derive(Debug)]
pub struct Object {
    pub key: String,
    pub data: Vec<u8>, // Stored as raw bytes
    // In a real S3, you might also have metadata like:
    // pub content_type: Option<String>,
    // pub etag: String, // Hash of the object's data
    // pub last_modified: std::time::SystemTime,
    // pub user_metadata: HashMap<String, String>,
}

impl Object {
    /// Creates a new Object instance.
    pub fn new(key: String, data: Vec<u8>) -> Self {
        Object {
            key,
            data,
            // Initialize other metadata fields if they were added
            // content_type: None,
            // etag: calculate_etag(&data),
            // last_modified: std::time::SystemTime::now(),
            // user_metadata: HashMap::new(),
        }
    }

    /// Returns the size of the object data in bytes.
    pub fn size(&self) -> usize {
        self.data.len()
    }
}