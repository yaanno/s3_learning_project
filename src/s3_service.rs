// s3_service.rs
// This module defines the core S3Service, which acts as our in-memory S3 system.
// It manages buckets and provides methods for S3-like operations.

use crate::bucket::Bucket; // Import the Bucket struct from our 'bucket' module
use crate::object::Object;
use std::collections::HashMap; // Import the Object struct from our 'object' module

/// Represents custom errors that can occur in our S3-like service.
#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum S3Error {
    BucketAlreadyExists,
    BucketNotFound,
    ObjectNotFound,
    InvalidOperation(String), // For general invalid operations with a message
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            S3Error::BucketAlreadyExists => write!(f, "Bucket already exists"),
            S3Error::BucketNotFound => write!(f, "Bucket not found"),
            S3Error::ObjectNotFound => write!(f, "Object not found"),
            S3Error::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
        }
    }
}

impl std::error::Error for S3Error {}

/// The main S3-like service structure.
/// It holds a collection of buckets, simulated in-memory using a HashMap.
pub struct S3Service {
    buckets: HashMap<String, Bucket>,
}

impl S3Service {
    /// Creates a new, empty S3Service instance.
    ///
    /// # Returns
    ///
    /// * `S3Service` - The newly created S3Service instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let s3_service = S3Service::new();
    /// ```
    pub fn new() -> Self {
        S3Service {
            buckets: HashMap::new(),
        }
    }

    /// Creates a new bucket with the given name.
    /// Returns Ok(()) on success, or an S3Error if the bucket already exists.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to create.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - Ok(()) on success, or an S3Error if the bucket already exists.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// ```
    pub fn create_bucket(&mut self, name: &str) -> Result<(), S3Error> {
        if self.buckets.contains_key(name) {
            return Err(S3Error::BucketAlreadyExists);
        }
        self.buckets
            .insert(name.to_string(), Bucket::new(name.to_string()));
        Ok(())
    }

    /// Deletes a bucket with the given name.
    /// Returns Ok(()) on success, or an S3Error if the bucket is not found.
    ///
    /// NOTE: In a real S3, you often cannot delete a non-empty bucket directly without a
    /// force option or by first deleting all its objects. For simplicity, this simulation
    /// allows deleting non-empty buckets.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the bucket to delete.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - Ok(()) on success, or an S3Error if the bucket is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// s3_service.delete_bucket("my-bucket").unwrap();
    /// ```
    pub fn delete_bucket(&mut self, name: &str) -> Result<(), S3Error> {
        if self.buckets.remove(name).is_some() {
            Ok(())
        } else {
            Err(S3Error::BucketNotFound)
        }
    }

    /// Lists the names of all existing buckets.
    ///
    /// # Returns
    ///
    /// * `Vec<String>` - A vector containing the names of all existing buckets.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// let buckets = s3_service.list_buckets();
    /// assert_eq!(buckets, vec!["my-bucket".to_string()]);
    /// ```
    pub fn list_buckets(&self) -> Vec<String> {
        self.buckets.keys().cloned().collect()
    }

    /// Puts an object into a specified bucket.
    /// Returns Ok(()) on success, or an S3Error if the bucket is not found.
    /// If the object key already exists, it will be overwritten (upsert behavior, like S3).
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket to which the object will be added.
    /// * `key` - The unique identifier for the object within its bucket.
    /// * `data` - The binary data of the object.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - Ok(()) on success, or an S3Error if the bucket is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// s3_service.put_object("my-bucket", "my-object-key", vec![1, 2, 3]).unwrap();
    /// let object = s3_service.get_object("my-bucket", "my-object-key").unwrap();
    /// assert_eq!(object.data, vec![1, 2, 3]);
    /// ```
    pub fn put_object(
        &mut self,
        bucket_name: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<&Object, S3Error> {
        if let Some(bucket) = self.buckets.get_mut(bucket_name) {
            let object = Object::new(key.to_string(), data);
            bucket.put_object(key.to_string(), object);
            let stored_object = bucket.get_object(key);
            match stored_object {
                Some(object) => Ok(object),
                None => Err(S3Error::ObjectNotFound),
            }
        } else {
            Err(S3Error::BucketNotFound)
        }
    }

    /// Retrieves an object from a specified bucket by its key.
    /// Returns the Object on success, or an S3Error if the bucket or object is not found.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket from which the object will be retrieved.
    /// * `key` - The unique identifier for the object within its bucket.
    ///
    /// # Returns
    ///
    /// * `Result<&Object, S3Error>` - The retrieved Object on success, or an S3Error if the bucket or object is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// s3_service.put_object("my-bucket", "my-object-key", vec![1, 2, 3]).unwrap();
    /// let object = s3_service.get_object("my-bucket", "my-object-key").unwrap();
    /// assert_eq!(object.data, vec![1, 2, 3]);
    /// ```
    pub fn get_object(&self, bucket_name: &str, key: &str) -> Result<&Object, S3Error> {
        if let Some(bucket) = self.buckets.get(bucket_name) {
            bucket.get_object(key).ok_or(S3Error::ObjectNotFound)
        } else {
            Err(S3Error::BucketNotFound)
        }
    }

    /// Deletes an object from a specified bucket by its key.
    /// Returns Ok(()) on success, or an S3Error if the bucket or object is not found.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket from which the object will be deleted.
    /// * `key` - The unique identifier for the object within its bucket.
    ///
    /// # Returns
    ///
    /// * `Result<(), S3Error>` - Ok(()) on success, or an S3Error if the bucket or object is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// s3_service.put_object("my-bucket", "my-object-key", vec![1, 2, 3]).unwrap();
    /// s3_service.delete_object("my-bucket", "my-object-key").unwrap();
    /// ```
    pub fn delete_object(&mut self, bucket_name: &str, key: &str) -> Result<(), S3Error> {
        if let Some(bucket) = self.buckets.get_mut(bucket_name) {
            if bucket.delete_object(key) {
                Ok(())
            } else {
                Err(S3Error::ObjectNotFound)
            }
        } else {
            Err(S3Error::BucketNotFound)
        }
    }

    /// Lists the keys of all objects within a specified bucket.
    /// Returns a Vec of object keys on success, or an S3Error if the bucket is not found.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket from which the objects will be listed.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<String>, S3Error>` - A vector of object keys on success, or an S3Error if the bucket is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use s3_learning_project::s3_service::S3Service;
    /// let mut s3_service = S3Service::new();
    /// s3_service.create_bucket("my-bucket").unwrap();
    /// s3_service.put_object("my-bucket", "my-object-key", vec![1, 2, 3]).unwrap();
    /// let objects = s3_service.list_objects("my-bucket").unwrap();
    /// assert_eq!(objects, vec!["my-object-key".to_string()]);
    /// ```
    pub fn list_objects(&self, bucket_name: &str) -> Result<Vec<String>, S3Error> {
        if let Some(bucket) = self.buckets.get(bucket_name) {
            Ok(bucket.list_objects())
        } else {
            Err(S3Error::BucketNotFound)
        }
    }
}
