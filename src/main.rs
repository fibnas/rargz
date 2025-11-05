use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use crossbeam_channel::{Receiver, Sender};
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use walkdir::WalkDir;
use zstd::bulk::Compressor;

/// Create a tar archive from `PATH` and compress it with zstd, writing to stdout.
#[derive(Debug, Parser)]
#[command(name = "rargz", version, about = "Parallel tar + zstd archiver")]
struct Args {
    /// Directory or file to archive.
    #[arg(value_name = "PATH")]
    input: PathBuf,

    /// Zstd compression level.
    #[arg(short = 'l', long, default_value_t = 3)]
    level: i32,

    /// Size of tar chunks fed into the compression queue (bytes).
    #[arg(long, value_name = "BYTES", default_value_t = 1 << 20)]
    chunk_size: usize,

    /// Number of worker threads for compression.
    #[arg(short = 'j', long, value_name = "THREADS")]
    jobs: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    run(args)
}

fn run(args: Args) -> Result<()> {
    if args.chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }

    let jobs = args.jobs.unwrap_or_else(default_jobs).max(1);

    let canonical_root = fs::canonicalize(&args.input)
        .with_context(|| format!("failed to resolve {}", args.input.display()))?;
    let tar_root = sanitize_tar_root(&args.input);
    let abort = AbortSignal::new();

    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<Chunk>(jobs * 4);
    let (result_tx, result_rx) = crossbeam_channel::bounded::<Result<CompressedChunk>>(jobs * 4);

    let writer_abort = abort.clone();
    let writer_handle = thread::Builder::new()
        .name("rargz-writer".to_string())
        .spawn(move || write_compressed_stream(result_rx, writer_abort))?;

    let pool = ThreadPoolBuilder::new()
        .num_threads(jobs)
        .thread_name(|index| format!("rargz-compress-{index}"))
        .build()
        .context("failed to create compression pool")?;

    for _ in 0..jobs {
        let worker_rx = chunk_rx.clone();
        let worker_tx = result_tx.clone();
        let worker_abort = abort.clone();
        let level = args.level;
        pool.spawn(move || compression_worker(worker_rx, worker_tx, worker_abort, level));
    }

    drop(result_tx);
    drop(chunk_rx);

    let tar_result = produce_tar_stream(
        &canonical_root,
        &tar_root,
        chunk_tx,
        args.chunk_size,
        &abort,
    );

    // Ensure all tasks complete before observing the outcome.
    drop(pool);

    let writer_result = match writer_handle.join() {
        Ok(res) => res,
        Err(_) => Err(anyhow!("writer thread panicked")),
    };

    // Prefer reporting the first failure in the pipeline.
    tar_result?;
    writer_result?;

    if abort.is_set() {
        bail!("pipeline aborted due to a prior error");
    }

    Ok(())
}

fn produce_tar_stream(
    canonical_root: &Path,
    tar_root: &Path,
    chunk_tx: Sender<Chunk>,
    chunk_size: usize,
    abort: &AbortSignal,
) -> Result<()> {
    let chunk_writer = ChunkWriter::new(chunk_tx, chunk_size, abort.clone());
    let mut builder = tar::Builder::new(chunk_writer);
    builder.follow_symlinks(false);
    builder.mode(tar::HeaderMode::Deterministic);

    write_entries(&mut builder, canonical_root, tar_root, abort)?;
    builder.finish().context("failed to finish tar archive")?;

    let mut chunk_writer = builder.into_inner().context("failed to flush tar writer")?;
    chunk_writer.flush_remaining()?;
    // Sender drops here, signalling EOF to the compression workers.

    Ok(())
}

fn write_entries<W: Write>(
    builder: &mut tar::Builder<W>,
    canonical_root: &Path,
    tar_root: &Path,
    abort: &AbortSignal,
) -> Result<()> {
    let walker = WalkDir::new(canonical_root).follow_links(false);

    for entry_result in walker {
        if abort.is_set() {
            bail!("archiving aborted");
        }

        let entry = entry_result?;
        let rel = entry
            .path()
            .strip_prefix(canonical_root)
            .context("walkdir produced a path outside of the root")?;

        let mut tar_path = PathBuf::new();
        if rel.as_os_str().is_empty() {
            tar_path.push(tar_root);
        } else {
            tar_path.push(tar_root);
            tar_path.push(rel);
        }

        builder
            .append_path_with_name(entry.path(), &tar_path)
            .with_context(|| format!("failed to archive {}", entry.path().display()))?;
    }

    Ok(())
}

fn compression_worker(
    chunk_rx: Receiver<Chunk>,
    result_tx: Sender<Result<CompressedChunk>>,
    abort: AbortSignal,
    level: i32,
) {
    let mut compressor = match Compressor::new(level) {
        Ok(c) => c,
        Err(err) => {
            abort.request();
            let _ = result_tx.send(Err(anyhow!("failed to create zstd compressor: {err}")));
            return;
        }
    };

    for chunk in chunk_rx.iter() {
        if abort.is_set() {
            break;
        }

        match compressor.compress(&chunk.data) {
            Ok(data) => {
                if result_tx
                    .send(Ok(CompressedChunk {
                        index: chunk.index,
                        data,
                    }))
                    .is_err()
                {
                    break;
                }
            }
            Err(err) => {
                abort.request();
                let _ = result_tx.send(Err(anyhow!("compression failed: {err}")));
                break;
            }
        }
    }
}

fn write_compressed_stream(
    result_rx: Receiver<Result<CompressedChunk>>,
    abort: AbortSignal,
) -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut pending = BTreeMap::<u64, Vec<u8>>::new();
    let mut next_index = 0u64;

    for item in result_rx {
        let chunk = match item {
            Ok(chunk) => chunk,
            Err(err) => {
                abort.request();
                return Err(err);
            }
        };

        pending.insert(chunk.index, chunk.data);

        while let Some(data) = pending.remove(&next_index) {
            if let Err(err) = stdout.write_all(&data) {
                abort.request();
                return Err(err.into());
            }
            next_index += 1;
        }
    }

    if !pending.is_empty() {
        abort.request();
        bail!("compression pipeline terminated with out-of-order chunks");
    }

    stdout.flush().context("failed to flush stdout")?;
    Ok(())
}

#[derive(Clone)]
struct AbortSignal(Arc<AtomicBool>);

impl AbortSignal {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    fn request(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    fn is_set(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

struct Chunk {
    index: u64,
    data: Vec<u8>,
}

struct CompressedChunk {
    index: u64,
    data: Vec<u8>,
}

struct ChunkWriter {
    tx: Sender<Chunk>,
    buffer: Vec<u8>,
    chunk_size: usize,
    next_index: u64,
    abort: AbortSignal,
}

impl ChunkWriter {
    fn new(tx: Sender<Chunk>, chunk_size: usize, abort: AbortSignal) -> Self {
        Self {
            tx,
            buffer: Vec::with_capacity(chunk_size),
            chunk_size,
            next_index: 0,
            abort,
        }
    }

    fn flush_remaining(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.send_chunk()?;
        }
        Ok(())
    }

    fn send_chunk(&mut self) -> io::Result<()> {
        if self.abort.is_set() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "compression aborted",
            ));
        }

        let mut data = Vec::with_capacity(self.buffer.len());
        data.extend_from_slice(&self.buffer);
        self.buffer.clear();
        let chunk = Chunk {
            index: self.next_index,
            data,
        };
        self.next_index += 1;

        self.tx
            .send(chunk)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "compression pipeline closed"))
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.abort.is_set() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "compression aborted",
            ));
        }

        let mut offset = 0;
        while offset < buf.len() {
            if self.buffer.len() == self.chunk_size {
                self.send_chunk()?;
            }

            let remaining = self.chunk_size - self.buffer.len();
            let to_copy = remaining.min(buf.len() - offset);
            self.buffer
                .extend_from_slice(&buf[offset..offset + to_copy]);
            offset += to_copy;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn sanitize_tar_root(input: &Path) -> PathBuf {
    let mut root = PathBuf::new();
    for component in input.components() {
        match component {
            Component::Prefix(prefix) => root.push(prefix.as_os_str()),
            Component::RootDir => {}
            Component::CurDir => {
                if root.as_os_str().is_empty() {
                    root.push(".");
                }
            }
            Component::ParentDir => root.push(".."),
            Component::Normal(part) => root.push(part),
        }
    }

    if root.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        root
    }
}

fn default_jobs() -> usize {
    num_cpus::get().max(1)
}
