// --- Request/Response Structs (for JSON where applicable) ---

use crate::object::Object;
use serde::Serialize;

// For listing buckets or objects
#[derive(Serialize)]
pub struct ListResponse {
    pub items: Vec<String>,
}

#[derive(Serialize)]
pub struct BucketCreatedResponse {
    pub name: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct BucketDeletedResponse {
    pub message: String,
    pub bucket: String,
}

#[derive(Serialize)]
pub struct ObjectCreatedResponse<'a> {
    pub name: String,
    pub bucket: String,
    pub metadata: &'a Object,
    pub message: String,
}

#[derive(Serialize)]
pub struct ObjectDeletedResponse {
    pub name: String,
    pub bucket: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct ObjectListResponse {
    pub bucket: String,
    pub items: Vec<String>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub message: String,
}
