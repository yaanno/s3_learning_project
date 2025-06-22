use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{error, info};

use crate::storage::{Storage, StorageError};

/// Background task that periodically checks storage consistency
pub struct ConsistencyChecker {
    storage: Arc<Mutex<Storage>>,
    check_interval: Duration,
}

impl ConsistencyChecker {
    /// Create a new ConsistencyChecker
    pub fn new(storage: Arc<Mutex<Storage>>, check_interval: Duration) -> Self {
        Self {
            storage,
            check_interval,
        }
    }

    /// Start the background consistency checker
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        // let storage = self.storage.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(self.check_interval);

            loop {
                interval.tick().await;

                match self.run_check().await {
                    Ok(_) => info!("Consistency check completed successfully"),
                    Err(e) => error!("Consistency check failed: {}", e),
                }
            }
        })
    }

    /// Run a single consistency check
    async fn run_check(&self) -> Result<(), StorageError> {
        let mut storage = self.storage.lock().await;
        storage.check_consistency()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_consistency_checker() {
        // Create a temporary directory for the test database
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db_path_str = db_path.to_str().unwrap();

        // Create storage and checker
        let storage = Storage::new(db_path_str).unwrap();
        let checker =
            ConsistencyChecker::new(Arc::new(Mutex::new(storage)), Duration::from_millis(100));

        // Start the checker
        let handle = checker.start();

        // Let it run for a short time
        sleep(Duration::from_millis(300)).await;

        // Cancel the task
        handle.abort();

        // Verify no panic occurred
        assert!(handle.await.unwrap_err().is_cancelled());
    }
}
