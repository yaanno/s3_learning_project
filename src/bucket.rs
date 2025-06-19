// bucket.rs
// This module defines the Bucket structure, representing an S3 bucket.
// Each bucket manages its own collection of objects.

use std::collections::HashMap;
use crate::object::Object; // Import the Object struct from our 'object' module

/// Represents an S3-like bucket.
/// Each bucket has a name and stores a collection of objects using a HashMap
/// where the key is the object's key (path) and the value is the Object itself.
pub struct Bucket {
    name: String,
    objects: HashMap<String, Object>, // Stores objects by their key
}

impl Bucket {
    /// Creates a new Bucket instance with the given name.
    pub fn new(name: String) -> Self {
        Bucket {
            name,
            objects: HashMap::new(),
        }
    }

    /// Returns the name of the bucket.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Stores an object in the bucket.
    /// If an object with the same key already exists, it will be overwritten.
    pub fn put_object(&mut self, key: String, object: Object) {
        self.objects.insert(key, object);
    }

    /// Retrieves a reference to an object from the bucket by its key.
    /// Returns `Some(&Object)` if found, `None` otherwise.
    pub fn get_object(&self, key: &str) -> Option<&Object> {
        self.objects.get(key)
    }

    /// Deletes an object from the bucket by its key.
    /// Returns `true` if the object was found and removed, `false` otherwise.
    pub fn delete_object(&mut self, key: &str) -> bool {
        self.objects.remove(key).is_some()
    }

    /// Lists the keys of all objects currently stored in the bucket.
    pub fn list_objects(&self) -> Vec<String> {
        self.objects.keys().cloned().collect()
    }

    /// Checks if the bucket is empty (contains no objects).
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

