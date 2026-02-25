use std::{
    process::exit,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use log::info;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

type Callbacks = Arc<Mutex<Vec<Box<dyn Fn() + Send + Sync>>>>;

static SIGNAL_RECEIVED: AtomicBool = AtomicBool::new(false);

extern "C" fn signal_handler(_sig: libc::c_int) {
    if SIGNAL_RECEIVED.swap(true, Ordering::Relaxed) {
        const MSG: &[u8] = b"Shutdown already pending...\n";
        unsafe { libc::write(2, MSG.as_ptr().cast(), MSG.len()) };
    } else {
        const MSG: &[u8] = b"Signal received, shutdown pending...\n";
        unsafe { libc::write(2, MSG.as_ptr().cast(), MSG.len()) };
    }
}

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

    /// Registers signal handlers for graceful shutdown (SIGINT + SIGTERM).
    ///
    /// # Panics
    /// Panics if `sigaction` fails to install handlers.
    pub fn set_ctrlc_handler(&self) {
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = signal_handler as *const () as usize;
            libc::sigemptyset(&raw mut action.sa_mask);
            action.sa_flags = libc::SA_RESTART;

            assert!(
                libc::sigaction(libc::SIGINT, &action, std::ptr::null_mut()) == 0,
                "failed to install SIGINT handler"
            );
            assert!(
                libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut()) == 0,
                "failed to install SIGTERM handler"
            );
        }
    }

    /// Acquires a read lock to protect a critical section from shutdown.
    /// The shutdown handler will wait for all locks to be released.
    ///
    /// If a signal was received while waiting, runs cleanup callbacks and exits.
    pub fn lock(&self) -> RwLockReadGuard<'_, ()> {
        let guard = self.lock.read();
        if SIGNAL_RECEIVED.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            for callback in self.cleanup_callbacks.lock().iter() {
                callback();
            }
            info!("Exiting...");
            exit(0);
        }
        guard
    }
}
