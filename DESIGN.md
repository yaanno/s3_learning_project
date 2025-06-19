# System Design Document: In-Memory S3-like Storage Service

## 1. Introduction
This document outlines the system design for a simplified, in-memory S3-like object storage service. The primary goal is to simulate core S3 concepts (buckets, objects, basic CRUD operations) for learning purposes within a Rust programming environment. This system is not intended for production use but serves as a foundation for understanding distributed storage principles.

## 2. Goals and Scope
The primary goals of this project are:

- To implement the fundamental concepts of S3: buckets and objects.

- To support basic operations:

- Create, list, and delete buckets.

- Put, get, list, and delete objects within a bucket.

- To provide an in-memory simulation, abstracting away network, disk I/O, and distributed complexities.

- To demonstrate modular Rust programming practices.

The current scope is limited to an in-memory, single-node application without persistence, concurrency, or advanced S3 features like versioning, access control lists (ACLs), or multi-part uploads.

## 3. High-Level Architecture
The system follows a simple client-server model, where the S3Service acts as the central "server" managing the storage.

+------------------+
|                  |
|    Application   | (e.g., main.rs)
|                  |
+--------+---------+
         |
         | Calls
         V
+------------------+
|                  |
|    S3Service     | (Manages Buckets)
|                  |
+--------+---------+
         |
         | Manages
         V
+------------------+
|                  |
|     Buckets      | (Each manages Objects)
|                  |
+--------+---------+
         |
         | Contains
         V
+------------------+
|                  |
|     Objects      | (Raw binary data)
|                  |
+------------------+

## 4. Component Breakdown

### 4.1. S3Service (Core Service)
Purpose: The central orchestrator for all S3-like operations. It acts as the public interface for interacting with the storage system.

*Responsibilities*

- Manages the collection of all Bucket instances.

- Handles requests for bucket creation, listing, and deletion.

- Delegates object operations (put, get, delete, list) to the appropriate Bucket instance.

- Manages error handling for service-level operations (e.g., BucketNotFound, BucketAlreadyExists).

Data Structures: Internally uses a HashMap<String, Bucket> to store buckets, mapping bucket names (Strings) to Bucket objects.

### 4.2. Bucket
Purpose: Represents an individual S3-like bucket.

*Responsibilities*

- Stores a collection of Object instances.

- Provides methods for putting, getting, listing, and deleting objects within itself.

- Manages its own name.

Data Structures: Internally uses a HashMap<String, Object> to store objects, mapping object keys (Strings) to Object objects.

### 4.3. Object
Purpose: Represents a single data item stored within a Bucket.

*Responsibilities*

- Holds the object's unique key within its bucket.

- Stores the actual binary data (Vec<u8>).

(Future: could hold metadata like content type, ETag, last modified date).

Data Structures: Simple struct containing a String for the key and Vec<u8> for data.

## 5. Error Handling
A custom S3Error enum is defined to provide specific error types for common scenarios:

- BucketAlreadyExists: When trying to create a bucket with a name that already exists.

- BucketNotFound: When an operation is attempted on a non-existent bucket.

- ObjectNotFound: When trying to retrieve or delete a non-existent object.

- InvalidOperation(String): A general error for other invalid operations, providing a descriptive message.

These errors are returned via Rust's Result type, enforcing explicit error handling by the caller.

## 6. Data Flow and Interactions

*Create Bucket*

- main.rs calls s3_service.create_bucket("name").

- S3Service checks if name already exists in its buckets HashMap.

- If not, it creates a new Bucket instance and inserts it into the HashMap. 

*Put Object*

- main.rs calls s3_service.put_object("bucket_name", "key", data).

- S3Service looks up bucket_name in its HashMap.

- If found, it calls bucket.put_object("key", new_object).

- Bucket stores or overwrites the Object in its internal HashMap.

*Get Object*

- main.rs calls s3_service.get_object("bucket_name", "key").

- S3Service looks up bucket_name.

- If found, it calls bucket.get_object("key").

Bucket retrieves the Object from its HashMap.

*Delete Bucket*

- main.rs calls s3_service.delete_bucket("name").

- S3Service removes the Bucket from its HashMap. (Note: Current implementation allows deleting non-empty buckets for simplicity; a real S3 would prevent this or require a "force" flag).

## 7. Future Enhancements

- Persistence: Save/load bucket and object data to/from disk (e.g., using JSON serialization with serde).

- Concurrency: Add std::sync::Mutex or tokio::sync::RwLock to S3Service and Bucket to allow thread-safe access.

- Asynchronous Operations: Convert to an asynchronous design using tokio for better performance and scalability.

- HTTP API: Build a basic RESTful API using a web framework like actix-web or warp.

- Advanced S3 Features: Implement object versioning, pre-signed URLs, object metadata, lifecycle policies.

- Testing: Comprehensive unit and integration tests.