//! Store-owned channel for batched file syncing.

use std::{
    fmt,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(not(msim))]
use std::sync::{
    Mutex,
    mpsc::{self, Receiver, Sender},
};

#[cfg(msim)]
use tokio::sync::{
    Mutex,
    mpsc::{self, UnboundedReceiver, UnboundedSender},
};

use crate::{Error, Result};

type Completion = Box<dyn FnOnce(Result<()>) + Send + 'static>;

#[cfg(not(msim))]
pub type FileSyncSender = Sender<FileSyncTask>;
#[cfg(msim)]
pub type FileSyncSender = UnboundedSender<FileSyncTask>;

/// One file sync followed by a caller-owned completion action.
pub struct FileSyncTask {
    path: PathBuf,
    file: File,
    completion: Completion,
}

impl FileSyncTask {
    pub fn new(
        path: impl AsRef<Path>,
        file: File,
        completion: impl FnOnce(Result<()>) + Send + 'static,
    ) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            file,
            completion: Box::new(completion),
        }
    }

    /// Finishes a task without running its file sync when it cannot be submitted or a prerequisite
    /// has already failed.
    pub(crate) fn complete(self, result: Result<()>) {
        (self.completion)(result);
    }

    fn run(self) {
        let Self {
            path,
            file,
            completion,
        } = self;
        let result = file
            .sync_data()
            .map_err(|source| Error::Io { path, source });
        completion(result);
    }
}

impl fmt::Debug for FileSyncTask {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileSyncTask")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// Creates a nonblocking task-submission channel and a cloneable worker.
pub fn file_sync_channel() -> (FileSyncSender, FileSyncer) {
    // Submission must never put filesystem latency or a full maintenance queue on the foreground
    // writer. The store permits only one durability publication at a time and independently caps
    // unsealed segments, which bounds the number of outstanding tasks without a bounded channel.
    #[cfg(not(msim))]
    let (sender, receiver) = mpsc::channel();
    #[cfg(msim)]
    let (sender, receiver) = mpsc::unbounded_channel();
    (
        sender,
        FileSyncer {
            receiver: Arc::new(Mutex::new(receiver)),
        },
    )
}

/// One worker over a shared file-sync task receiver.
#[derive(Clone)]
pub struct FileSyncer {
    #[cfg(not(msim))]
    receiver: Arc<Mutex<Receiver<FileSyncTask>>>,
    #[cfg(msim)]
    receiver: Arc<Mutex<UnboundedReceiver<FileSyncTask>>>,
}

impl FileSyncer {
    /// Runs tasks until every sender is dropped.
    ///
    /// Clones may run concurrently. The receiver lock is released before syncing the file and
    /// invoking its completion action.
    #[cfg(not(msim))]
    pub fn run(self) {
        while let Some(task) = self.recv() {
            task.run();
        }
    }

    #[cfg(not(msim))]
    fn recv(&self) -> Option<FileSyncTask> {
        self.receiver
            .lock()
            .expect("file sync receiver lock poisoned")
            .recv()
            .ok()
    }

    /// Receives on the simulated executor and performs only the finite file sync in the
    /// deterministic blocking pool. The completion callback therefore re-enters the writer from
    /// an msim-owned task instead of an unmanaged native worker thread.
    #[cfg(msim)]
    pub async fn run(self) {
        while let Some(task) = self.recv().await {
            match tokio::task::spawn_blocking(move || task.run()).await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => return,
                Err(error) => panic!("Strata file-sync task failed: {error}"),
            }
        }
    }

    #[cfg(msim)]
    async fn recv(&self) -> Option<FileSyncTask> {
        self.receiver.lock().await.recv().await
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write, sync::mpsc, thread};

    use tempfile::tempdir;

    use super::{FileSyncTask, file_sync_channel};

    #[test]
    fn workers_share_the_receiver_and_complete_every_task() {
        let dir = tempdir().unwrap();
        let (task_tx, syncer) = file_sync_channel();
        let workers = (0..2)
            .map(|_| {
                let syncer = syncer.clone();
                thread::spawn(move || syncer.run())
            })
            .collect::<Vec<_>>();
        drop(syncer);
        let (completion_tx, completion_rx) = mpsc::channel();

        for id in 1..=4 {
            let path = dir.path().join(format!("{id}.data"));
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.write_all(format!("file-{id}").as_bytes()).unwrap();
            let completion_tx = completion_tx.clone();
            task_tx
                .send(FileSyncTask::new(path, file, move |result| {
                    completion_tx.send((id, result)).unwrap();
                }))
                .unwrap();
        }

        drop(completion_tx);
        drop(task_tx);
        let mut completed = completion_rx.into_iter().collect::<Vec<_>>();
        completed.sort_by_key(|(id, _)| *id);
        assert_eq!(
            completed
                .into_iter()
                .map(|(id, result)| (id, result.is_ok()))
                .collect::<Vec<_>>(),
            vec![(1, true), (2, true), (3, true), (4, true)]
        );
        for worker in workers {
            worker.join().unwrap();
        }
    }
}
