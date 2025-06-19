// main.rs
// This is the main entry point of the Rust application.
// It sets up and demonstrates the usage of our simplified S3-like service.

mod s3_service; // Declare the s3_service module
mod bucket;     // Declare the bucket module
mod object;     // Declare the object module

use s3_service::{S3Service, S3Error};

fn main() {
    println!("Starting S3-like Storage System Simulation...");

    // Create a new instance of our S3-like service
    let mut s3 = S3Service::new();

    // --- Demonstrate Bucket Operations ---

    println!("\n--- Bucket Operations ---");

    // 1. Create a bucket
    match s3.create_bucket("my-first-bucket") {
        Ok(_) => println!("Successfully created bucket: 'my-first-bucket'"),
        Err(e) => eprintln!("Failed to create bucket: {}", e),
    }

    // Try to create the same bucket again (should fail)
    match s3.create_bucket("my-first-bucket") {
        Ok(_) => println!("Successfully created bucket: 'my-first-bucket' (unexpected)"),
        Err(e) => println!("Failed to create bucket (as expected): {}", e),
    }

    // Create another bucket
    match s3.create_bucket("my-second-bucket") {
        Ok(_) => println!("Successfully created bucket: 'my-second-bucket'"),
        Err(e) => eprintln!("Failed to create bucket: {}", e),
    }

    // 2. List buckets
    println!("\nListing all buckets:");
    let buckets = s3.list_buckets();
    if buckets.is_empty() {
        println!("No buckets found.");
    } else {
        for bucket_name in buckets {
            println!(" - {}", bucket_name);
        }
    }

    // --- Demonstrate Object Operations ---

    println!("\n--- Object Operations ---");

    let bucket_name = "my-first-bucket";

    // 1. Put an object into a bucket
    let object_key_1 = "documents/report.txt";
    let object_data_1 = "This is the content of my important report.".as_bytes().to_vec();
    match s3.put_object(bucket_name, object_key_1, object_data_1.clone()) {
        Ok(_) => println!("Successfully put object '{}' into bucket '{}'", object_key_1, bucket_name),
        Err(e) => eprintln!("Failed to put object: {}", e),
    }

    let object_key_2 = "images/profile.jpg";
    let object_data_2 = vec![0xDE, 0xAD, 0xBE, 0xEF]; // Simulate binary data
    match s3.put_object(bucket_name, object_key_2, object_data_2.clone()) {
        Ok(_) => println!("Successfully put object '{}' into bucket '{}'", object_key_2, bucket_name),
        Err(e) => eprintln!("Failed to put object: {}", e),
    }

    // Try to put an object into a non-existent bucket
    let non_existent_bucket = "non-existent-bucket";
    match s3.put_object(non_existent_bucket, "test.txt", "data".as_bytes().to_vec()) {
        Ok(_) => println!("Successfully put object (unexpected)"),
        Err(e) => println!("Failed to put object into non-existent bucket (as expected): {}", e),
    }

    // 2. Get an object from a bucket
    println!("\nGetting object '{}' from bucket '{}':", object_key_1, bucket_name);
    match s3.get_object(bucket_name, object_key_1) {
        Ok(object) => {
            println!("  Content: '{}'", String::from_utf8_lossy(&object.data));
            println!("  Size: {} bytes", object.data.len());
        },
        Err(e) => eprintln!("Failed to get object: {}", e),
    }

    println!("\nGetting non-existent object 'nonexistent.txt' from bucket '{}':", bucket_name);
    match s3.get_object(bucket_name, "nonexistent.txt") {
        Ok(_) => println!("Successfully got non-existent object (unexpected)"),
        Err(e) => println!("Failed to get non-existent object (as expected): {}", e),
    }

    // 3. List objects in a bucket
    println!("\nListing objects in bucket '{}':", bucket_name);
    match s3.list_objects(bucket_name) {
        Ok(keys) => {
            if keys.is_empty() {
                println!("  No objects found in this bucket.");
            } else {
                for key in keys {
                    println!("  - {}", key);
                }
            }
        },
        Err(e) => eprintln!("Failed to list objects: {}", e),
    }

    println!("\nListing objects in non-existent bucket '{}':", non_existent_bucket);
    match s3.list_objects(non_existent_bucket) {
        Ok(_) => println!("Successfully listed objects in non-existent bucket (unexpected)"),
        Err(e) => println!("Failed to list objects in non-existent bucket (as expected): {}", e),
    }

    // 4. Delete an object from a bucket
    println!("\nDeleting object '{}' from bucket '{}':", object_key_1, bucket_name);
    match s3.delete_object(bucket_name, object_key_1) {
        Ok(_) => println!("Successfully deleted object '{}'", object_key_1),
        Err(e) => eprintln!("Failed to delete object: {}", e),
    }

    // Try to delete a non-existent object
    println!("\nDeleting non-existent object 'nonexistent.txt' from bucket '{}':", bucket_name);
    match s3.delete_object(bucket_name, "nonexistent.txt") {
        Ok(_) => println!("Successfully deleted non-existent object (unexpected)"),
        Err(e) => println!("Failed to delete non-existent object (as expected): {}", e),
    }

    // List objects again after deletion
    println!("\nListing objects in bucket '{}' after deletion:", bucket_name);
    match s3.list_objects(bucket_name) {
        Ok(keys) => {
            if keys.is_empty() {
                println!("  No objects found in this bucket.");
            } else {
                for key in keys {
                    println!("  - {}", key);
                }
            }
        },
        Err(e) => eprintln!("Failed to list objects: {}", e),
    }

    // --- Demonstrate Bucket Deletion ---

    println!("\n--- Bucket Deletion ---");

    // Try to delete a non-empty bucket (should fail in a real S3)
    // For simplicity, our current implementation allows deleting non-empty buckets.
    // In a real S3, you'd need to delete all objects first or use a force flag.
    println!("\nAttempting to delete 'my-first-bucket' (still contains an object):");
    match s3.delete_bucket(bucket_name) {
        Ok(_) => println!("Successfully deleted bucket: '{}'", bucket_name),
        Err(e) => eprintln!("Failed to delete bucket: {}", e),
    }

    // Verify deletion
    println!("\nListing all buckets after deletion attempt:");
    let buckets_after_delete = s3.list_buckets();
    if buckets_after_delete.is_empty() {
        println!("No buckets found.");
    } else {
        for name in buckets_after_delete {
            println!(" - {}", name);
        }
    }

    // Delete the second bucket
    match s3.delete_bucket("my-second-bucket") {
        Ok(_) => println!("Successfully deleted bucket: 'my-second-bucket'"),
        Err(e) => eprintln!("Failed to delete bucket: {}", e),
    }

    println!("\nFinal check: Listing all buckets:");
    let final_buckets = s3.list_buckets();
    if final_buckets.is_empty() {
        println!("No buckets found.");
    } else {
        for bucket_name in final_buckets {
            println!(" - {}", bucket_name);
        }
    }

    println!("\nSimulation Finished.");
}