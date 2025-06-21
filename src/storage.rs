// storage.rs
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::time::SystemTime;
use serde_json;
use md5::{Digest, Md5};
use hex;
use thiserror::Error;

use crate::object::Object;

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
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] rusqlite::Error),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("System time error: {0}")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Transaction failed to commit")]
    TransactionCommitError,
    #[error("Invalid path: {0}")]
    InvalidPath(String),
    #[error("Object '{0}' not found in bucket '{1}'")]
    ObjectNotFound(String, String),
    #[error("Bucket '{0}' already exists in storage")]
    BucketAlreadyExistsInStorage(String),
    #[error("Bucket '{0}' not found in storage")] // <--- NEW: Specific error for bucket not found in storage
    BucketNotFoundInStorage(String),
}

impl Storage {
    pub fn new(db_path: &str) -> Result<Self, StorageError> {
        let conn = Connection::open(db_path)?;
        let base_path = Path::new("data").to_path_buf();

        fs::create_dir_all(&base_path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                bucket_name TEXT,
                key TEXT,
                file_path TEXT UNIQUE,
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

    pub fn create_bucket(&mut self, bucket_name: &str) -> Result<(), StorageError> {
        let tx = self.conn.transaction()?;
        match tx.execute(
            "INSERT INTO buckets (name) VALUES (?1)",
            [bucket_name],
        ) {
            Ok(_) => {
                tx.commit().map_err(|e| StorageError::DatabaseError(e))?;
                Ok(())
            },
            Err(rusqlite::Error::SqliteFailure(e, Some(msg))) if e.code == rusqlite::ErrorCode::ConstraintViolation && msg.contains("UNIQUE constraint failed: buckets.name") => {
                tx.rollback().map_err(|e| StorageError::DatabaseError(e))?;
                Err(StorageError::BucketAlreadyExistsInStorage(bucket_name.to_string()))
            },
            Err(e) => {
                tx.rollback().map_err(|err| StorageError::DatabaseError(err))?;
                Err(StorageError::DatabaseError(e))
            },
        }
    }

    pub fn _delete_bucket(&mut self, bucket: &str) -> Result<(), StorageError> {
        let tx = self.conn.transaction()?;
        let rows_affected = tx.execute(
            "DELETE FROM buckets WHERE name = ?1",
            [bucket],
        )?;
        if rows_affected == 0 {
            tx.rollback().map_err(|e| StorageError::DatabaseError(e))?;
            return Err(StorageError::BucketNotFoundInStorage(bucket.to_string()));
        }
        tx.commit().map_err(|_| StorageError::TransactionCommitError)
    }

    pub fn list_buckets(&self) -> Result<Vec<String>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT name FROM buckets",
        )?;
        let mut rows = stmt.query([])?;
        let mut bucket_names = Vec::new();
        while let Some(row) = rows.next()? {
            bucket_names.push(row.get(0)?);
        }
        Ok(bucket_names)
    }

    // <--- NEW: Method to check if a bucket exists directly
    pub fn bucket_exists(&self, bucket_name: &str) -> Result<bool, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT 1 FROM buckets WHERE name = ?1",
        )?;
        let exists: Option<i64> = stmt.query_row(params![bucket_name], |row| row.get(0)).optional()?;
        Ok(exists.is_some())
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
        let etag = calculate_etag(&object.data);

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
                etag,
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

        let mut rows = stmt.query(params![bucket, key])?;

        let row = rows.next()?;
        if let Some(row) = row {
            let file_path_str: String = row.get(0)?;
            let file_path = PathBuf::from(file_path_str);
            let content_type: Option<String> = row.get(1)?;
            let etag: String = row.get(2)?;
            let last_modified: i64 = row.get(3)?;
            let metadata_json: Option<String> = row.get(4)?;

            let data = fs::read(&file_path)?;

            let user_metadata: Option<HashMap<String, String>> = metadata_json
                .map(|s| serde_json::from_str(&s))
                .transpose()?;

            Ok(Object {
                key: key.to_string(),
                data,
                content_type,
                etag,
                last_modified,
                user_metadata,
            })
        } else {
            Err(StorageError::ObjectNotFound(key.to_string(), bucket.to_string()))
        }
    }

    pub fn delete_object(&mut self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let file_path_to_delete_option: Option<String> = self.conn.query_row(
            "SELECT file_path FROM objects WHERE bucket_name = ?1 AND key = ?2",
            params![bucket, key],
            |row| row.get(0),
        ).optional()?;

        let tx = self.conn.transaction()?;

        let rows_affected = tx.execute(
            "DELETE FROM objects WHERE bucket_name = ?1 AND key = ?2",
            params![bucket, key],
        )?;

        if rows_affected > 0 {
            if let Some(file_path_str) = file_path_to_delete_option {
                let file_path = PathBuf::from(file_path_str);
                if file_path.exists() {
                    fs::remove_file(&file_path)?;
                }
            }
            tx.commit().map_err(|_| StorageError::TransactionCommitError)?;
            Ok(true)
        } else {
            tx.rollback()?;
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

    pub fn _is_empty(&self, bucket: &str) -> Result<bool, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT COUNT(*) FROM objects WHERE bucket_name = ?1",
        )?;
        let count: i64 = stmt.query_row(params![bucket], |row| row.get(0))?;
        Ok(count == 0)
    }
}