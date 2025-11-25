use std::{process::exit, sync::Arc};

use log::info;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

type Callbacks = Arc<Mutex<Vec<Box<dyn Fn() + Send + Sync>>>>;

/// Graceful shutdown coordinator for ensuring data consistency during program exit.
///
/// Uses a read-write lock to coordinate between operations and shutdown signals (e.g., Ctrl-C).
/// Operations hold read locks during critical sections, preventing shutdown until they complete.
/// Registered rollbacks will be ran on exit.
#[derive(Default, Clone)]
pub struct Exit {
    lock: Arc<RwLock<()>>,
    cleanup_callbacks: Callbacks,
}

impl Exit {
    pub fn new() -> Self {
        Self {
            lock: Arc::new(RwLock::new(())),
            cleanup_callbacks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Registers a callback to be executed during shutdown.
    /// Callbacks are executed in registration order before the program exits.
    pub fn register_cleanup<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.cleanup_callbacks.lock().push(Box::new(callback));
    }

    /// Sets the Ctrl-C signal handler for graceful shutdown.
    ///
    /// # Panics
    /// Only one Ctrl-C handler can be registered per process. This will panic if:
    /// - Called multiple times
    /// - Another crate has already registered a Ctrl-C handler
    pub fn set_ctrlc_handler(&self) {
        let lock_copy = self.lock.clone();
        let callbacks = self.cleanup_callbacks.clone();

        ctrlc::set_handler(move || {
            // Run cleanup callbacks
            for callback in callbacks.lock().iter() {
                callback();
            }

            if let Some(_lock) = lock_copy.try_write() {
                info!("Exiting...");
                exit(0);
            }

            info!("Waiting to exit safely...");
            let _lock = lock_copy.write();
            info!("Exiting...");
            exit(0);
        })
        .expect("Error setting Ctrl-C handler");
    }

    /// Acquires a read lock to protect a critical section from shutdown.
    /// The shutdown handler will wait for all locks to be released.
    pub fn lock(&self) -> RwLockReadGuard<'_, ()> {
        self.lock.read()
    }
}
