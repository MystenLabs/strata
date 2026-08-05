//! Store-owned channel for batched file syncing.

use std::{
    fmt,
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
};

use crate::{Error, Result};

type Completion = Box<dyn FnOnce(Result<()>) + Send + 'static>;

pub type FileSyncSender = Sender<FileSyncTask>;

/// One file-sync result that can be subscribed to before or after the worker completes it.
///
/// Rollover starts syncing a closed segment immediately, while a later durability snapshot owns
/// the callback that counts that segment toward its completion barrier. Keeping the result here
/// lets those two events happen in either order without a thread blocking on a receiver.
#[derive(Clone)]
pub(crate) struct FileSyncCompletion {
    inner: Arc<Mutex<FileSyncCompletionState>>,
}

#[derive(Default)]
struct FileSyncCompletionState {
    finished: bool,
    result: Option<Result<()>>,
    listener: Option<Completion>,
}

impl FileSyncCompletion {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(FileSyncCompletionState::default())),
        }
    }

    pub(crate) fn complete(&self, result: Result<()>) {
        let listener = {
            let mut state = self
                .inner
                .lock()
                .expect("file sync completion lock poisoned");
            assert!(!state.finished, "file sync completed more than once");
            state.finished = true;
            match state.listener.take() {
                Some(listener) => Some((listener, result)),
                None => {
                    state.result = Some(result);
                    None
                }
            }
        };
        if let Some((listener, result)) = listener {
            listener(result);
        }
    }

    pub(crate) fn notify(&self, listener: impl FnOnce(Result<()>) + Send + 'static) {
        let listener: Completion = Box::new(listener);
        let completed = {
            let mut state = self
                .inner
                .lock()
                .expect("file sync completion lock poisoned");
            assert!(
                state.listener.is_none(),
                "file sync completion already has a listener"
            );
            if state.finished {
                Some((
                    listener,
                    state.result.take().expect("completed result missing"),
                ))
            } else {
                state.listener = Some(listener);
                None
            }
        };
        if let Some((listener, result)) = completed {
            listener(result);
        }
    }
}

impl fmt::Debug for FileSyncCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self
            .inner
            .lock()
            .expect("file sync completion lock poisoned");
        formatter
            .debug_struct("FileSyncCompletion")
            .field("finished", &state.finished)
            .field("has_listener", &state.listener.is_some())
            .finish()
    }
}

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
    let (sender, receiver) = mpsc::channel();
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
    receiver: Arc<Mutex<Receiver<FileSyncTask>>>,
}

impl FileSyncer {
    /// Runs tasks until every sender is dropped.
    ///
    /// Clones may run concurrently. The receiver lock is released before syncing the file and
    /// invoking its completion action.
    pub fn run(self) {
        while let Some(task) = self.recv() {
            let FileSyncTask {
                path,
                file,
                completion,
            } = task;
            let result = file
                .sync_data()
                .map_err(|source| Error::Io { path, source });
            completion(result);
        }
    }

    fn recv(&self) -> Option<FileSyncTask> {
        self.receiver
            .lock()
            .expect("file sync receiver lock poisoned")
            .recv()
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write, sync::mpsc, thread};

    use tempfile::tempdir;

    use super::{FileSyncCompletion, FileSyncTask, file_sync_channel};

    #[test]
    fn completion_notifies_before_or_after_worker_finishes() {
        for complete_first in [false, true] {
            let completion = FileSyncCompletion::new();
            let (notify_tx, notify_rx) = mpsc::channel();
            if complete_first {
                completion.complete(Ok(()));
            }
            completion.notify(move |result| notify_tx.send(result).unwrap());
            if !complete_first {
                completion.complete(Ok(()));
            }
            notify_rx.recv().unwrap().unwrap();
        }
    }

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
