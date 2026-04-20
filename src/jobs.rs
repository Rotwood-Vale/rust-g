//! Job system
use flume::Receiver;
use std::{
    cell::RefCell,
    collections::hash_map::{Entry, HashMap},
    thread,
};

struct Job {
    rx: Receiver<Output>,
    handle: thread::JoinHandle<()>,
}

type Output = String;
type JobId = String;

const NO_RESULTS_YET: &str = "NO RESULTS YET";
const NO_SUCH_JOB: &str = "NO SUCH JOB";
const JOB_PANICKED: &str = "JOB PANICKED";

#[derive(Default)]
struct Jobs {
    map: HashMap<JobId, Job>,
    next_job: usize,
}

impl Jobs {
fn start<F: FnOnce() -> Output + Send + 'static>(&mut self, f: F, desc: String) -> JobId {
    let (tx, rx) = flume::unbounded();
    let id = self.next_job.to_string();
    self.next_job += 1;

    match std::thread::Builder::new()
        .name(format!("rust-g job {} ({})", id, desc))
        .spawn(move || {
            let _ = tx.send(f());
        })
    {
        Ok(handle) => {
            self.map.insert(id.clone(), Job { rx, handle });
        }
        Err(e) => {
            eprintln!(
                "rust-g: thread spawn failed for job {} ({}) | OS error: {} (code: {:?}) | active jobs in map: {}",
                id, desc, e, e.raw_os_error(), self.map.len()
            );
        }
    }

    id
}

    fn check(&mut self, id: &str) -> Output {
        let entry = match self.map.entry(id.to_owned()) {
            Entry::Occupied(occupied) => occupied,
            Entry::Vacant(_) => return NO_SUCH_JOB.to_owned(),
        };
        let result = match entry.get().rx.try_recv() {
            Ok(result) => result,
            Err(flume::TryRecvError::Disconnected) => JOB_PANICKED.to_owned(),
            Err(flume::TryRecvError::Empty) => return NO_RESULTS_YET.to_owned(),
        };
        let _ = entry.remove().handle.join();
        result
    }
}

thread_local! {
    static JOBS: RefCell<Jobs> = RefCell::default();
}

pub fn start<F: FnOnce() -> Output + Send + 'static>(f: F, desc: impl Into<String>) -> JobId {
    JOBS.with(|jobs| jobs.borrow_mut().start(f, desc.into()))
}

pub fn check(id: &str) -> String {
    JOBS.with(|jobs| jobs.borrow_mut().check(id))
}
