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
    /// 
    /// # Arguments
    /// 
    /// * `name` - The name of the bucket.
    /// 
    /// # Returns
    /// 
    /// * `Bucket` - The newly created Bucket instance.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// let bucket = Bucket::new("my-bucket".to_string());
    /// ```
    pub fn new(name: String) -> Self {
        Bucket {
            name,
            objects: HashMap::new(),
        }
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
    /// let bucket = Bucket::new("my-bucket".to_string());
    /// assert_eq!(bucket.get_name(), "my-bucket");
    /// ```
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Stores an object in the bucket.
    /// If an object with the same key already exists, it will be overwritten.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The unique identifier for the object within its bucket.
    /// * `object` - The Object to store in the bucket.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::object::Object;
    /// let mut bucket = Bucket::new("my-bucket".to_string());
    /// let object = Object::new("my-object-key".to_string(), vec![1, 2, 3]);
    /// bucket.put_object("my-object-key".to_string(), object);
    /// ```
    pub fn put_object(&mut self, key: String, object: Object) {
        self.objects.insert(key, object);
    }

    /// Retrieves a reference to an object from the bucket by its key.
    /// Returns `Some(&Object)` if found, `None` otherwise.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The unique identifier for the object within its bucket.
    /// 
    /// # Returns
    /// 
    /// * `Option<&Object>` - A reference to the object if found, `None` otherwise.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// use s3_learning_project::object::Object;
    /// let mut bucket = Bucket::new("my-bucket".to_string());
    /// let object = Object::new("my-object-key".to_string(), vec![1, 2, 3]);
    /// bucket.put_object("my-object-key".to_string(), object);
    /// let retrieved_object = bucket.get_object("my-object-key");
    /// assert!(retrieved_object.is_some());
    /// ```
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
    /// 
    /// # Returns
    /// 
    /// * `bool` - `true` if the bucket is empty, `false` otherwise.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use s3_learning_project::bucket::Bucket;
    /// let bucket = Bucket::new("my-bucket".to_string());
    /// assert!(bucket.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

