use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::info;

use crate::executor;
use crate::parser::{self, QueryPlan};

/// A job submitted to the scheduler.
struct Job {
    id: u64,
    query: String,
    resp: oneshot::Sender<JobResult>,
    cost: usize,
}

/// Scheduler managing job execution with a maximum number of concurrent jobs.
#[derive(Clone)]
pub struct Scheduler {
    tx: mpsc::Sender<Job>,
    active: Arc<AtomicUsize>,
    next_id: Arc<AtomicU64>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct JobResult {
    pub bytes: Option<Vec<u8>>, // compressed
    pub path: Option<String>,
    pub duration: Duration,
    pub cost: usize,
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

        Scheduler {
            tx,
            active,
            next_id,
        }
    }

    fn estimate_cost(plan: &[QueryPlan]) -> usize {
        // very rough heuristic: each step costs 10 units
        plan.len() * 10
    }

    /// Enqueue a new job and return its id, status and channel to await results.
    pub async fn enqueue(
        &self,
        query: String,
    ) -> (u64, &'static str, oneshot::Receiver<JobResult>) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let plan = parser::parse_query(&query).unwrap_or_default();
        let cost = Self::estimate_cost(&plan);
        let (tx, rx) = oneshot::channel();
        let status = if self.active.load(Ordering::SeqCst) < 4 {
            "running"
        } else {
            "queued"
        };
        let job = Job {
            id,
            query,
            resp: tx,
            cost,
        };
        // Ignore send errors - only possible if scheduler loop has shut down.
        let _ = self.tx.send(job).await;
        (id, status, rx)
    }
}

/// Spawn a task to execute a job and notify when complete.
fn spawn_job(job: Job, complete: mpsc::Sender<()>, active: Arc<AtomicUsize>) {
    active.fetch_add(1, Ordering::SeqCst);
    tokio::spawn(async move {
        let start = Instant::now();
        info!(job_id = job.id, "job started");
        let result = executor::execute_plan(&job.query);
        let duration = start.elapsed();
        info!(job_id = job.id, ?duration, "job finished");

        let job_result = if let Ok(df) = result {
            match crate::utils::prepare_output(job.id, &df) {
                Ok(o) => JobResult {
                    bytes: o.bytes,
                    path: o.path,
                    duration,
                    cost: job.cost,
                },
                Err(_) => JobResult {
                    bytes: None,
                    path: None,
                    duration,
                    cost: job.cost,
                },
            }
        } else {
            JobResult {
                bytes: None,
                path: None,
                duration,
                cost: job.cost,
            }
        };

        let _ = job.resp.send(job_result);
        let _ = complete.send(()).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::ParquetWriter;
    use polars::prelude::*;
    use std::fs::File;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn enqueue_and_complete() {
        let sched = Scheduler::new();
        let mut df = df!["name" => ["a"], "age" => [10]].unwrap();
        let file = NamedTempFile::new().unwrap();
        ParquetWriter::new(File::create(file.path()).unwrap())
            .finish(&mut df)
            .unwrap();
        let query = format!(
            "df = pl.read_parquet(\"{}\")",
            file.path().to_str().unwrap()
        );
        let (_id, _status, rx) = sched.enqueue(query).await;
        let res = rx.await.unwrap();
        assert!(res.bytes.is_some() || res.path.is_some());
        assert!(res.cost > 0);
    }
}
