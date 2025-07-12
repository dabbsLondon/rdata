use std::collections::VecDeque;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};

use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::info;

use crate::executor;

/// A job submitted to the scheduler.
struct Job {
    id: u64,
    query: String,
}

/// Scheduler managing job execution with a maximum number of concurrent jobs.
#[derive(Clone)]
pub struct Scheduler {
    tx: mpsc::Sender<Job>,
    active: Arc<AtomicUsize>,
    next_id: Arc<AtomicU64>,
}

impl Scheduler {
    /// Create a new scheduler and spawn the background worker.
    pub fn new() -> Self {
        let (tx, mut rx) = mpsc::channel::<Job>(100);
        let (complete_tx, mut complete_rx) = mpsc::channel::<()>(100);
        let active = Arc::new(AtomicUsize::new(0));
        let next_id = Arc::new(AtomicU64::new(1));
        let active_bg = active.clone();

        tokio::spawn(async move {
            let mut queue: VecDeque<Job> = VecDeque::new();
            loop {
                tokio::select! {
                    Some(job) = rx.recv() => {
                        if active_bg.load(Ordering::SeqCst) < 4 {
                            spawn_job(job, complete_tx.clone(), active_bg.clone());
                        } else {
                            queue.push_back(job);
                        }
                    }
                    Some(_) = complete_rx.recv() => {
                        active_bg.fetch_sub(1, Ordering::SeqCst);
                        if let Some(job) = queue.pop_front() {
                            spawn_job(job, complete_tx.clone(), active_bg.clone());
                        }
                    }
                    else => break,
                }
            }
        });

        Scheduler { tx, active, next_id }
    }

    /// Enqueue a new job and return its id and whether it starts immediately or
    /// is queued.
    pub async fn enqueue(&self, query: String) -> (u64, &'static str) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let status = if self.active.load(Ordering::SeqCst) < 4 {
            "running"
        } else {
            "queued"
        };
        let job = Job { id, query };
        // Ignore send errors - only possible if scheduler loop has shut down.
        let _ = self.tx.send(job).await;
        (id, status)
    }
}

/// Spawn a task to execute a job and notify when complete.
fn spawn_job(job: Job, complete: mpsc::Sender<()>, active: Arc<AtomicUsize>) {
    active.fetch_add(1, Ordering::SeqCst);
    tokio::spawn(async move {
        let start = Instant::now();
        info!(job_id = job.id, "job started");
        executor::execute_plan(&job.query);
        let duration = start.elapsed();
        info!(job_id = job.id, ?duration, "job finished");
        let _ = complete.send(()).await;
    });
}
