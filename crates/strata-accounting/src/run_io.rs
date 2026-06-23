use std::{
    fs,
    io::{BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use strata_core::StrataLsn;

use crate::manifest::RunKind;
use crate::state::{BlobUpdate, PatchRecord, RecordLsn, StateRecord};
use crate::{Error, FORMAT_VERSION, PartitionId, Result, RunId};

const RUN_MAGIC: &[u8; 8] = b"STRACC01";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunHeader {
    format_version: u32,
    pub(crate) kind: RunKind,
    pub(crate) partition: PartitionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RunRecords {
    State(Vec<StateRecord>),
    Patch(Vec<PatchRecord>),
    Delta(Vec<BlobUpdate>),
}

/// Decoded image of a physical run file payload.
///
/// This private struct mirrors bytes written by `write_run_file_atomic`: a temporary file is fully
/// framed and synced, then renamed into its partition directory. The file becomes live only through a
/// `RunMeta` in the durable manifest, and it becomes obsolete when compaction publishes a manifest
/// that no longer references that metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunFile {
    /// The run file has its own format fence so an otherwise-valid manifest cannot make the reader
    /// deserialize records with an incompatible on-disk layout.
    pub(crate) format_version: u32,
    /// The payload shape controls which record type is legal in the file. It duplicates manifest
    /// metadata on purpose so corruption or cross-linking is detected when the file is opened.
    pub(crate) kind: RunKind,
    /// The partition is embedded in the file header as a second guard against a manifest row pointing
    /// at a run from another hash range.
    pub(crate) partition: PartitionId,
    /// Records are physically homogeneous inside one run. The enum preserves the LSM level boundary:
    /// base files hold folded state, patch files hold residual per-key history, and delta files hold
    /// raw sorted updates.
    pub(crate) records: RunRecords,
}

pub(crate) struct RunRecordReader<T> {
    path: PathBuf,
    pub(crate) header: RunHeader,
    reader: BufReader<fs::File>,
    _record: PhantomData<T>,
}

impl<T> Iterator for RunRecordReader<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        read_record_frame(&mut self.reader, &self.path).transpose()
    }
}

pub(crate) fn partition_dir(root: &Path, partition: PartitionId) -> PathBuf {
    root.join(format!("partition-{partition:05}"))
}

pub(crate) fn run_file_name(kind: RunKind, run_id: RunId) -> String {
    let prefix = match kind {
        RunKind::Base => "base",
        RunKind::Patch => "patch",
        RunKind::Delta => "delta",
    };
    format!("{prefix}-{run_id:020}.run")
}

pub(crate) fn open_run_record_reader<T>(path: &Path) -> Result<RunRecordReader<T>>
where
    T: DeserializeOwned,
{
    let mut reader = BufReader::new(fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?);
    let header = read_run_header(&mut reader, path)?;
    Ok(RunRecordReader {
        path: path.to_path_buf(),
        header,
        reader,
        _record: PhantomData,
    })
}

pub(crate) fn write_run_file_atomic<I, T>(
    path: &Path,
    kind: RunKind,
    partition: PartitionId,
    records: I,
) -> Result<(Option<StrataLsn>, u64)>
where
    I: IntoIterator<Item = Result<T>>,
    T: Serialize + RecordLsn,
{
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    let tmp_path = path.with_extension("tmp");
    let mut max_lsn: Option<StrataLsn> = None;
    {
        let file = fs::File::create(&tmp_path).map_err(|source| Error::Io {
            path: tmp_path.clone(),
            source,
        })?;
        let mut writer = BufWriter::new(file);
        write_run_header(
            &mut writer,
            &RunHeader {
                format_version: FORMAT_VERSION,
                kind,
                partition,
            },
            &tmp_path,
        )?;
        for record in records {
            let record = record?;
            max_lsn = Some(max_lsn.map_or(record.record_lsn(), |max| max.max(record.record_lsn())));
            write_record_frame(&mut writer, &record, &tmp_path)?;
        }
        writer.flush().map_err(|source| Error::Io {
            path: tmp_path.clone(),
            source,
        })?;
        writer.get_ref().sync_all().map_err(|source| Error::Io {
            path: tmp_path.clone(),
            source,
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let file_len = fs::metadata(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    sync_parent_dir(path)?;
    Ok((max_lsn, file_len))
}

fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let dir = fs::File::open(parent).map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| Error::Io {
        path: parent.to_path_buf(),
        source,
    })
}

fn write_run_header(writer: &mut impl Write, header: &RunHeader, path: &Path) -> Result<()> {
    writer.write_all(RUN_MAGIC).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    write_record_frame(writer, header, path)
}

fn read_run_header(reader: &mut impl Read, path: &Path) -> Result<RunHeader> {
    let mut magic = [0; 8];
    reader.read_exact(&mut magic).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if &magic != RUN_MAGIC {
        return Err(Error::CorruptRun {
            path: path.to_path_buf(),
            reason: "invalid run magic".to_owned(),
        });
    }
    let header: RunHeader = read_record_frame(reader, path)?.ok_or_else(|| Error::CorruptRun {
        path: path.to_path_buf(),
        reason: "missing run header".to_owned(),
    })?;
    if header.format_version != FORMAT_VERSION {
        return Err(Error::IncompatibleManifestVersion {
            actual: header.format_version,
            expected: FORMAT_VERSION,
        });
    }
    Ok(header)
}

pub(crate) fn write_record_frame<T: Serialize>(
    writer: &mut impl Write,
    record: &T,
    path: &Path,
) -> Result<()> {
    let bytes = bcs::to_bytes(record)?;
    let len =
        u32::try_from(bytes.len()).map_err(|_| Error::RunFrameTooLarge { len: bytes.len() })?;
    writer
        .write_all(&len.to_le_bytes())
        .and_then(|_| writer.write_all(&bytes))
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })
}

pub(crate) fn read_record_frame<T: DeserializeOwned>(
    reader: &mut impl Read,
    path: &Path,
) -> Result<Option<T>> {
    let mut first = [0; 1];
    match reader.read(&mut first) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("one-byte buffer cannot read more than one byte"),
        Err(source) => {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    }

    let mut len = [0; 4];
    len[0] = first[0];
    reader
        .read_exact(&mut len[1..])
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let len = u32::from_le_bytes(len) as usize;
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(Some(bcs::from_bytes(&bytes)?))
}
