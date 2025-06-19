# Implementation Document: In-Memory S3-like Storage Service

## 1. Project Structure
The project is organized into a standard Rust cargo project with the following file structure:
``` 
s3_learning_project/
├── Cargo.toml
└── src/
    ├── main.rs         // Main application entry point and demonstration
    ├── s3_service.rs   // Defines the core S3Service struct and its methods
    ├── bucket.rs       // Defines the Bucket struct and its methods
    └── object.rs       // Defines the Object struct
```
## 2. Module Breakdown and Implementation Details

### 2.1. src/main.rs
Purpose: The main executable file that imports the other modules and demonstrates the functionality of the S3Service.

*Key Logic*

Initializes a mutable S3Service instance.

Contains a sequence of calls to S3Service methods (`create_bucket`, `put_object`, `get_object`, `list_buckets`, `list_objects`, `delete_object`, `delete_bucket`).

Uses Rust's match statement to handle Result types returned by the S3Service methods, printing success or error messages to the console.

Simulates various scenarios including successful operations, attempts to create duplicate buckets, get non-existent objects, etc., to show error handling.

### 2.2. src/s3_service.rs
Purpose: Implements the S3Service struct, which acts as the main interface to our in-memory storage system.

*Dependencies*

Imports `std::collections::HashMap`, `crate::bucket::Bucket`, and `crate::object::Object`.

*Error Handling*

S3Error Enum

`#[derive(Debug, PartialEq)]`: Allows for easy debugging and comparison in tests.

`impl std::fmt::Display for S3Error`: Enables user-friendly printing of error messages.

`impl std::error::Error for S3Error`: Marks S3Error as a standard Rust error type, allowing it to be used with ? operator (though not explicitly used in main.rs due to direct match statements).

*Data Structures*

S3Service Struct:

buckets: HashMap<String, Bucket>: The core data store, mapping bucket names to Bucket instances. HashMap is chosen for its efficient key-based lookups, simulating fast access to buckets.

*Methods*

- `new() -> Self`: Constructor, initializes an empty HashMap.

- `create_bucket(&mut self, name: &str) -> Result<(), S3Error>`:

Checks self.buckets.contains_key(name) to prevent duplicate bucket creation.

Inserts a Bucket::new(name.to_string()) into the HashMap.

- `delete_bucket(&mut self, name: &str) -> Result<(), S3Error>`:

Uses self.buckets.remove(name) which returns Option<Bucket>. is_some() checks if the bucket was found and removed.

- `list_buckets(&self) -> Vec<String>`:

Collects cloned keys from self.buckets.keys() into a Vec<String>.

- `put_object(&mut self, bucket_name: &str, key: &str, data: Vec<u8>) -> Result<(), S3Error>`:

`self.buckets.get_mut(bucket_name)`: Obtains a mutable reference to the target bucket.

If bucket exists, calls bucket.put_object(key.to_string(), Object::new(key.to_string(), data)). This demonstrates delegation of object management to the Bucket.

- `get_object(&self, bucket_name: &str, key: &str) -> Result<&Object, S3Error>`:

- `bucket.get_object(key).ok_or(S3Error::ObjectNotFound)`: Converts Option<&Object> to Result<&Object, S3Error>.

- `delete_object(&mut self, bucket_name: &str, key: &str) -> Result<(), S3Error>`:

Delegates to bucket.delete_object(key).

- `list_objects(&self, bucket_name: &str) -> Result<Vec<String>, S3Error>`:

Delegates to bucket.list_objects().

### 2.3. src/bucket.rs
Purpose: Implements the Bucket struct, responsible for managing objects within a single bucket.

*Dependencies*

Imports `std::collections::HashMap` and `crate::object::Object`.

*Bucket Struct*

name: String: The unique name of the bucket.

objects: HashMap<String, Object>: Stores objects, mapping their keys to Object instances.

*Methods*

- `new(name: String) -> Self`: Constructor.

- `get_name(&self) -> &str`: Getter for the bucket name.

- `put_object(&mut self, key: String, object: Object)`: Inserts or overwrites an object.

- `get_object(&self, key: &str) -> Option<&Object>`: Retrieves an immutable reference to an object.

- `delete_object(&mut self, key: &str) -> bool`: Removes an object, returning true if it was present.

- `list_objects(&self) -> Vec<String>`: Returns a list of all object keys in the bucket.

- `is_empty(&self) -> bool`: Checks if the bucket contains any objects.

### 2.4. src/object.rs
Purpose: Implements the Object struct, representing the actual data stored.

*Object Struct*

pub key: String: The object's key (path) within its bucket.

pub data: Vec<u8>: The raw binary content of the object. Vec<u8> is suitable for any type of data (text, image, video, etc.).

#[derive(Debug)]: Allows easy printing of the object for debugging.

*Methods*

- `new(key: String, data: Vec<u8>) -> Self`: Constructor.

- `size(&self) -> usize`: Returns the byte length of the data vector.
