use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::SystemTime;
use serde_json;
use serde::Serialize;
use md5::{Digest, Md5};
use hex;
use thiserror::Error;

use crate::object::Object; // Assuming Object has key, data, content_type, etag, last_modified, user_metadata fields

pub struct Storage {
    conn: Connection,
    base_path: PathBuf,
}

fn calculate_etag(data: &[u8]) -> String {
    let mut hasher = Md5::default();
    hasher.input(data);
    hex::encode(hasher.result())
}

/// Custom error type for operations within the storage module.
#[derive(Debug, Error, Serialize)]
pub enum StorageError {
    #[error("Database error: {0}")]
    #[serde(skip_serializing)]
    DatabaseError(#[from] rusqlite::Error),
    #[error("I/O error: {0}")]
    #[serde(skip_serializing)]
    IoError(#[from] std::io::Error),
    #[error("System time error: {0}")]
    #[serde(skip_serializing)]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("JSON serialization/deserialization error: {0}")]
    #[serde(skip_serializing)]
    JsonError(#[from] serde_json::Error),
    #[error("Transaction failed to commit")]
    TransactionCommitError,
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Object '{0}' not found in bucket '{1}'")]
    ObjectNotFound(String, String),
    // #[error("Metadata field missing or invalid for object '{0}' in bucket '{1}'")]
    // ObjectMetadataMissing(String, String), // New error for when file is found but metadata is bad
}

impl Storage {
    pub fn new(db_path: &str) -> Result<Self, StorageError> {
        let conn = Connection::open(db_path)?;
        let base_path = Path::new("data").to_path_buf();

        fs::create_dir_all(&base_path)?;
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                bucket_name TEXT,
                key TEXT,
                file_path TEXT UNIQUE, -- Added UNIQUE constraint as file_path should be unique
                content_type TEXT,
                etag TEXT,
                size INTEGER,
                last_modified TIMESTAMP,
                metadata TEXT,
                PRIMARY KEY (bucket_name, key),
                FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            )",
            [],
        )?;

        Ok(Self { conn, base_path })
    }

    pub fn put_object(
        &mut self,
        bucket: &str,
        object: Object,
    ) -> Result<(), StorageError> {
        let tx = self.conn.transaction()?;
        
        tx.execute(
            "INSERT OR IGNORE INTO buckets (name) VALUES (?1)",
            [bucket],
        )?;

        let bucket_dir = self.base_path.join("buckets").join(bucket);
        fs::create_dir_all(&bucket_dir)?;
        
        let file_path = bucket_dir.join(&object.key);

        let file_path_str = file_path.to_str()
            .ok_or_else(|| StorageError::InvalidPath(file_path.display().to_string()))?
            .to_string();

        fs::write(&file_path, &object.data)?;
        
        let metadata_json = match &object.user_metadata {
            Some(map) => Some(serde_json::to_string(map)?),
            None => None,
        };

        let size = object.data.len() as i64;
        let etag = calculate_etag(&object.data); // Recalculate ETag here to ensure it matches stored data
        
        let last_modified = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs() as i64;

        tx.execute(
            "INSERT OR REPLACE INTO objects 
             (bucket_name, key, file_path, content_type, etag, size, last_modified, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                bucket,
                object.key,
                file_path_str,
                object.content_type,
                etag, // Use the recalculated etag here
                size,
                last_modified,
                metadata_json
            ],
        )?;

        tx.commit().map_err(|_| StorageError::TransactionCommitError)?;
        Ok(())
    }
    
    pub fn get_object(&self, bucket: &str, key: &str) -> Result<Object, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT file_path, content_type, etag, last_modified, metadata
             FROM objects WHERE bucket_name = ?1 AND key = ?2",
        )?;
        // Changed `size` column not selected, as Object constructor typically takes `data: Vec<u8>`
        // and calculates size internally.

        let mut rows = stmt.query(params![bucket, key])?;

        let row = rows.next()?;
        if let Some(row) = row {
            let file_path_str: String = row.get(0)?;
            let file_path = PathBuf::from(file_path_str);
            let content_type: Option<String> = row.get(1)?;
            let etag: String = row.get(2)?;
            let last_modified: i64 = row.get(3)?;
            let metadata_json: Option<String> = row.get(4)?;

            let data = fs::read(&file_path)?; // Read actual data from file system

            let user_metadata: Option<HashMap<String, String>> = metadata_json
                .map(|s| serde_json::from_str(&s))
                .transpose()?; // Handle JSON deserialization error

            Ok(Object {
                key: key.to_string(),
                data,
                content_type,
                etag, // Use the etag from the DB, not recalculated here
                last_modified,
                user_metadata,
            })
        } else {
            Err(StorageError::ObjectNotFound(key.to_string(), bucket.to_string()))
        }
    }

    pub fn delete_object(&mut self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        // First, get the file path to delete the actual file
        let file_path_to_delete_option: Option<String> = self.conn.query_row(
            "SELECT file_path FROM objects WHERE bucket_name = ?1 AND key = ?2",
            params![bucket, key],
            |row| row.get(0),
        ).optional()?; // Use .optional() to handle no rows found without erroring immediately

        let tx = self.conn.transaction()?;

        let rows_affected = tx.execute(
            "DELETE FROM objects WHERE bucket_name = ?1 AND key = ?2",
            params![bucket, key],
        )?;

        if rows_affected > 0 {
            // Only attempt to delete the file if an object was actually removed from DB
            if let Some(file_path_str) = file_path_to_delete_option {
                let file_path = PathBuf::from(file_path_str);
                if file_path.exists() { // Check if the file actually exists before trying to remove
                    fs::remove_file(&file_path)?; // `?` handles std::io::Error
                }
            } else {
                // This case should ideally not happen if rows_affected > 0,
                // but adding a log or specific error might be useful for robustness.
                // For simplicity, we'll proceed as if the file was implicitly gone.
            }
            tx.commit().map_err(|_| StorageError::TransactionCommitError)?;
            Ok(true)
        } else {
            // No rows affected means object was not found to delete
            tx.rollback()?; // Rollback the transaction since no changes were made/committed
            Err(StorageError::ObjectNotFound(key.to_string(), bucket.to_string()))
        }
    }

    pub fn list_objects(&self, bucket: &str) -> Result<Vec<String>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT key FROM objects WHERE bucket_name = ?1",
        )?;
        let mut rows = stmt.query(params![bucket])?;
        let mut object_keys = Vec::new();
        while let Some(row) = rows.next()? {
            object_keys.push(row.get(0)?);
        }
        Ok(object_keys)
    }

    pub fn is_empty(&self, bucket: &str) -> Result<bool, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT COUNT(*) FROM objects WHERE bucket_name = ?1",
        )?;
        let count: i64 = stmt.query_row(params![bucket], |row| row.get(0))?;
        Ok(count == 0)
    }
}