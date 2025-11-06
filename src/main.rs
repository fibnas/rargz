use anyhow::{Context, Result, anyhow, bail};
use atty::Stream;
use clap::Parser;
use crossbeam_channel::{Receiver, Sender};
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::fs;
use std::convert::TryFrom;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
use walkdir::WalkDir;
use zstd::bulk::{Compressor, Decompressor};
use zstd::stream::read::Decoder as ZstdDecoder;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};

/// Create a tar archive from `PATH` and compress it with zstd, writing to stdout.
#[derive(Debug, Clone, clap::ValueEnum)]
enum ArchiveFormat {
    TarZst,
    Rargz,
}

#[derive(Debug, Parser)]
#[command(name = "rargz", version, about = "Parallel tar + zstd archiver/extractor")]
struct Args {
    /// Directory or file to archive.
    #[arg(
        value_name = "PATH",
        required_unless_present_any = ["extract", "list", "count"]
    )]
    input: Option<PathBuf>,

    /// Zstd compression level.
    #[arg(short = 'l', long, default_value_t = 3)]
    level: i32,

    /// Size of tar chunks fed into the compression queue (bytes).
    #[arg(long, value_name = "BYTES", default_value_t = 1 << 20)]
    chunk_size: usize,

    /// Number of worker threads for compression.
    #[arg(short = 'j', long, value_name = "THREADS")]
    jobs: Option<usize>,

    /// Extract from stdin into the output directory.
    #[arg(short = 'x', long, conflicts_with_all = ["list", "count"])]
    extract: bool,

    /// Output directory for extraction.
    #[arg(short = 'o', long, value_name = "DIR", requires = "extract")]
    output: Option<PathBuf>,

    /// Output format for the archive.
    #[arg(long, default_value_t = ArchiveFormat::TarZst, value_enum)]
    format: ArchiveFormat,

    /// List the archive contents from stdin.
    #[arg(long, conflicts_with_all = ["extract", "count"])]
    list: bool,

    /// Count items in the archive from stdin.
    #[arg(long, conflicts_with_all = ["extract", "list"])]
    count: bool,

    /// Disable progress reporting.
    #[arg(long)]
    no_progress: bool,
}

fn progress_allowed(no_progress: bool) -> bool {
    !no_progress && atty::is(Stream::Stderr)
}

#[derive(Clone)]
struct ProgressReporter {
    inner: Option<Arc<ProgressInner>>,
}

struct ProgressInner {
    bar: ProgressBar,
    files: AtomicU64,
    bytes: AtomicU64,
    finished: AtomicBool,
}

enum InputFormat {
    TarZst,
    Rargz { chunk_size: usize },
}

enum InspectionMode {
    List,
    Count,
}

impl ProgressReporter {
    fn new(label: impl Into<String>, enabled: bool) -> Self {
        if !enabled {
            return Self { inner: None };
        }

        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template("{prefix:.bold} {spinner} {wide_msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        bar.enable_steady_tick(Duration::from_millis(120));
        let label = label.into();
        bar.set_prefix(label);
        let inner = Arc::new(ProgressInner {
            bar,
            files: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        });
        inner.update_message();
        Self { inner: Some(inner) }
    }

    fn record_file(&self) {
        if let Some(inner) = &self.inner {
            inner.files.fetch_add(1, Ordering::Relaxed);
            inner.update_message();
        }
    }

    fn add_bytes(&self, bytes: u64) {
        if let Some(inner) = &self.inner {
            inner.bytes.fetch_add(bytes, Ordering::Relaxed);
            inner.update_message();
        }
    }

    fn finish_success(&self) {
        if let Some(inner) = &self.inner {
            inner.finish_with_message("done");
        }
    }

    fn finish_error(&self) {
        if let Some(inner) = &self.inner {
            inner.finish_with_message("failed");
        }
    }
}

impl ProgressInner {
    fn update_message(&self) {
        let files = self.files.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        self.bar
            .set_message(format!("{files} files - {}", HumanBytes(bytes)));
    }

    fn finish_with_message(&self, status: &str) {
        if self
            .finished
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.bar
                .finish_with_message(format!("{status} - {}", self.bar.message()));
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    run(args)
}

fn run(args: Args) -> Result<()> {
    if args.extract {
        let output = args.output.clone().unwrap_or_else(|| PathBuf::from("."));
        let progress = ProgressReporter::new("extracting", progress_allowed(args.no_progress));
        let result = run_extract(&args, output, progress.clone());
        match &result {
            Ok(_) => progress.finish_success(),
            Err(_) => progress.finish_error(),
        }
        return result;
    }

    if args.list {
        return run_inspect(&args, InspectionMode::List);
    }

    if args.count {
        return run_inspect(&args, InspectionMode::Count);
    }

    if args.chunk_size == 0 {
        bail!("chunk-size must be greater than zero");
    }

    let progress = ProgressReporter::new("archiving", progress_allowed(args.no_progress));
    let result = run_archive(&args, progress.clone());
    match &result {
        Ok(_) => progress.finish_success(),
        Err(_) => progress.finish_error(),
    }
    result
}

fn run_archive(args: &Args, progress: ProgressReporter) -> Result<()> {
    let jobs = args.jobs.unwrap_or_else(default_jobs).max(1);

    let input = args
        .input
        .as_ref()
        .context("PATH is required unless --extract is provided")?;

    let canonical_root = fs::canonicalize(input)
        .with_context(|| format!("failed to resolve {}", input.display()))?;
    let tar_root = sanitize_tar_root(input);
    let abort = AbortSignal::new();

    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<Chunk>(jobs * 4);
    let (result_tx, result_rx) = crossbeam_channel::bounded::<Result<CompressedChunk>>(jobs * 4);

    let writer_abort = abort.clone();
    let writer_handle = thread::Builder::new()
        .name("rargz-writer".to_string())
        .spawn({
            let format = args.format.clone();
            let progress = progress.clone();
            let chunk_size = args.chunk_size;
            move || write_compressed_stream(result_rx, writer_abort, format, chunk_size, progress)
        })?;

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
        progress.clone(),
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

fn run_extract(args: &Args, output: PathBuf, progress: ProgressReporter) -> Result<()> {
    let jobs = args.jobs.unwrap_or_else(default_jobs).max(1);

    if let Some(path) = args.input.as_ref() {
        let file = fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        return extract_from_reader(BufReader::new(file), jobs, &output, progress);
    }

    if atty::is(Stream::Stdin) {
        bail!("no input provided; pipe an archive or pass PATH");
    }

    let stdin = io::stdin();
    let reader = BufReader::new(stdin.lock());
    extract_from_reader(reader, jobs, &output, progress)
}

fn extract_from_reader<R>(
    mut reader: R,
    jobs: usize,
    output: &Path,
    progress: ProgressReporter,
) -> Result<()>
where
    R: Read + BufRead,
{
    match detect_input_format(&mut reader)? {
        InputFormat::TarZst => extract_tar_zst(reader, output, progress),
        InputFormat::Rargz { chunk_size } => extract_rargz(reader, output, jobs, chunk_size, progress),
    }
}

fn run_inspect(args: &Args, mode: InspectionMode) -> Result<()> {
    if let Some(path) = args.input.as_ref() {
        let file = fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        return inspect_from_reader(BufReader::new(file), mode);
    }

    if atty::is(Stream::Stdin) {
        bail!("no input provided; pipe an archive or pass PATH");
    }

    let stdin = io::stdin();
    inspect_from_reader(BufReader::new(stdin.lock()), mode)
}

fn inspect_from_reader<R>(mut reader: R, mode: InspectionMode) -> Result<()>
where
    R: Read + BufRead,
{
    match detect_input_format(&mut reader)? {
        InputFormat::TarZst => {
            let decoder =
                ZstdDecoder::new(reader).context("failed to initialize zstd decoder")?;
            inspect_tar_stream(decoder, mode)
        }
        InputFormat::Rargz { chunk_size } => {
            let seq_reader =
                RargzSequentialReader::new(reader, chunk_size).context("failed to prepare rargz reader")?;
            inspect_tar_stream(seq_reader, mode)
        }
    }
}

fn inspect_tar_stream<R: Read>(reader: R, mode: InspectionMode) -> Result<()> {
    let mut archive = tar::Archive::new(reader);
    let mut entries = archive
        .entries()
        .context("failed to iterate over tar entries")?;
    let mut total = 0u64;

    while let Some(entry) = entries.next() {
        let mut entry = entry?;
        let path = entry
            .path()
            .context("failed to resolve entry path")?
            .to_path_buf();
        total += 1;
        if let InspectionMode::List = mode {
            println!("{}", path.display());
        }
        io::copy(&mut entry, &mut io::sink()).context("failed to advance past archive entry")?;
    }

    if let InspectionMode::Count = mode {
        println!("{total}");
    }

    Ok(())
}

fn detect_input_format<R: BufRead>(reader: &mut R) -> Result<InputFormat> {
    let buffer = reader.fill_buf()?;
    if buffer.len() >= RARGZ_MAGIC.len() && &buffer[..RARGZ_MAGIC.len()] == RARGZ_MAGIC {
        reader.consume(RARGZ_MAGIC.len());

        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        if version[0] != RARGZ_VERSION {
            bail!("unsupported rargz version {}", version[0]);
        }

        let mut chunk_buf = [0u8; 8];
        reader.read_exact(&mut chunk_buf)?;
        let chunk_size = u64::from_le_bytes(chunk_buf);
        let chunk_size = usize::try_from(chunk_size).context("chunk size exceeds addressable space")?;
        if chunk_size == 0 {
            bail!("invalid chunk size found in rargz header");
        }

        Ok(InputFormat::Rargz { chunk_size })
    } else {
        Ok(InputFormat::TarZst)
    }
}

fn extract_tar_zst<R: Read>(reader: R, output: &Path, progress: ProgressReporter) -> Result<()> {
    let decoder = ZstdDecoder::new(reader).context("failed to initialize zstd decoder")?;
    extract_tar_from_reader(decoder, output, progress)
}

fn extract_rargz<R: Read>(
    mut reader: R,
    output: &Path,
    jobs: usize,
    chunk_size: usize,
    progress: ProgressReporter,
) -> Result<()> {
    let abort = AbortSignal::new();
    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<CompressedChunk>(jobs * 4);
    let (plain_tx, plain_rx) = crossbeam_channel::bounded::<Result<Chunk>>(jobs * 4);

    let pool = ThreadPoolBuilder::new()
        .num_threads(jobs)
        .thread_name(|index| format!("rargz-decompress-{index}"))
        .build()
        .context("failed to create decompression pool")?;

    for _ in 0..jobs {
        let worker_rx = chunk_rx.clone();
        let worker_tx = plain_tx.clone();
        let worker_abort = abort.clone();
        pool.spawn(move || decompression_worker(worker_rx, worker_tx, worker_abort, chunk_size));
    }

    drop(plain_tx);
    drop(chunk_rx);

    let extraction_progress = progress.clone();
    let output_path = output.to_path_buf();
    let extraction_abort = abort.clone();
    let extractor_handle = thread::Builder::new()
        .name("rargz-untar".to_string())
        .spawn(move || {
            let reader = ChunkStreamReader::new(plain_rx, extraction_abort);
            extract_tar_from_reader(reader, &output_path, extraction_progress)
        })?;

    let feed_result = feed_rargz_chunks(&mut reader, &chunk_tx, &abort);
    drop(chunk_tx);

    drop(pool);

    let extraction_result = match extractor_handle.join() {
        Ok(res) => res,
        Err(_) => Err(anyhow!("extraction thread panicked")),
    };

    feed_result?;
    extraction_result?;

    if abort.is_set() {
        bail!("extraction aborted due to a prior error");
    }

    Ok(())
}

fn feed_rargz_chunks<R: Read>(
    reader: &mut R,
    chunk_tx: &Sender<CompressedChunk>,
    abort: &AbortSignal,
) -> Result<()> {
    let mut index = 0u64;
    let mut len_buf = [0u8; 8];

    loop {
        if abort.is_set() {
            return Ok(());
        }

        match reader.read_exact(&mut len_buf) {
            Ok(()) => {
                let chunk_len = u64::from_le_bytes(len_buf) as usize;
                if chunk_len == 0 {
                    continue;
                }
                let mut data = vec![0u8; chunk_len];
                reader
                    .read_exact(&mut data)
                    .with_context(|| format!("failed to read chunk {index}"))?;
                chunk_tx
                    .send(CompressedChunk { index, data })
                    .map_err(|_| anyhow!("decompression pipeline closed"))?;
                index += 1;
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }

    Ok(())
}

fn extract_tar_from_reader<R: Read>(
    reader: R,
    output: &Path,
    progress: ProgressReporter,
) -> Result<()> {
    let mut archive = tar::Archive::new(reader);
    let mut entries = archive.entries().context("failed to iterate over tar entries")?;

    while let Some(entry) = entries.next() {
        let mut entry = entry?;
        let size = entry.size();
        let path = entry
            .path()
            .context("failed to resolve entry path")?
            .to_path_buf();
        progress.record_file();
        entry
            .unpack_in(output)
            .with_context(|| format!("failed to unpack {}", path.display()))?;
        progress.add_bytes(size);
    }

    Ok(())
}

fn produce_tar_stream(
    canonical_root: &Path,
    tar_root: &Path,
    chunk_tx: Sender<Chunk>,
    chunk_size: usize,
    abort: &AbortSignal,
    progress: ProgressReporter,
) -> Result<()> {
    let chunk_writer = ChunkWriter::new(chunk_tx, chunk_size, abort.clone(), progress.clone());
    let mut builder = tar::Builder::new(chunk_writer);
    builder.follow_symlinks(false);
    builder.mode(tar::HeaderMode::Deterministic);

    write_entries(&mut builder, canonical_root, tar_root, abort, &progress)?;
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
    progress: &ProgressReporter,
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

        progress.record_file();
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

fn decompression_worker(
    chunk_rx: Receiver<CompressedChunk>,
    result_tx: Sender<Result<Chunk>>,
    abort: AbortSignal,
    chunk_capacity: usize,
) {
    let mut decompressor = match Decompressor::new() {
        Ok(d) => d,
        Err(err) => {
            abort.request();
            let _ = result_tx.send(Err(anyhow!("failed to create zstd decompressor: {err}")));
            return;
        }
    };

    for chunk in chunk_rx.iter() {
        if abort.is_set() {
            break;
        }

        match decompressor.decompress(&chunk.data, chunk_capacity) {
            Ok(data) => {
                if result_tx
                    .send(Ok(Chunk {
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
                let _ = result_tx.send(Err(anyhow!("decompression failed: {err}")));
                break;
            }
        }
    }
}

const RARGZ_MAGIC: &[u8; 6] = b"RARGZ\0";
const RARGZ_VERSION: u8 = 1;

fn write_compressed_stream(
    result_rx: Receiver<Result<CompressedChunk>>,
    abort: AbortSignal,
    format: ArchiveFormat,
    chunk_size: usize,
    _progress: ProgressReporter,
) -> Result<()> {
    let mut stdout = io::stdout().lock();
    let mut pending = BTreeMap::<u64, Vec<u8>>::new();
    let mut next_index = 0u64;

    if let ArchiveFormat::Rargz = format {
        stdout.write_all(RARGZ_MAGIC)?;
        stdout.write_all(&[RARGZ_VERSION])?;
        stdout.write_all(&(chunk_size as u64).to_le_bytes())?;
    }

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
            if let ArchiveFormat::Rargz = format {
                let len = data.len() as u64;
                if let Err(err) = stdout.write_all(&len.to_le_bytes()) {
                    abort.request();
                    return Err(err.into());
                }
            }

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

struct ChunkStreamReader {
    rx: Receiver<Result<Chunk>>,
    pending: BTreeMap<u64, Vec<u8>>,
    next_index: u64,
    current: Option<Vec<u8>>,
    offset: usize,
    abort: AbortSignal,
}

impl ChunkStreamReader {
    fn new(rx: Receiver<Result<Chunk>>, abort: AbortSignal) -> Self {
        Self {
            rx,
            pending: BTreeMap::new(),
            next_index: 0,
            current: None,
            offset: 0,
            abort,
        }
    }

    fn ensure_buffer(&mut self) -> io::Result<bool> {
        loop {
            if let Some(ref data) = self.current {
                if self.offset < data.len() {
                    return Ok(true);
                } else {
                    self.current = None;
                    self.offset = 0;
                }
            }

            if let Some(data) = self.pending.remove(&self.next_index) {
                self.next_index += 1;
                self.current = Some(data);
                continue;
            }

            if self.abort.is_set() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "extraction aborted",
                ));
            }

            match self.rx.recv() {
                Ok(Ok(chunk)) => {
                    if chunk.index == self.next_index {
                        self.next_index += 1;
                        self.current = Some(chunk.data);
                    } else {
                        self.pending.insert(chunk.index, chunk.data);
                    }
                }
                Ok(Err(err)) => {
                    self.abort.request();
                    return Err(io::Error::new(io::ErrorKind::Other, err.to_string()));
                }
                Err(_) => return Ok(false),
            }
        }
    }
}

impl Read for ChunkStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if !self.ensure_buffer()? {
            return Ok(0);
        }

        let data = self.current.as_ref().expect("buffer should be available");
        let available = &data[self.offset..];
        let to_copy = available.len().min(buf.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        self.offset += to_copy;
        if self.offset == data.len() {
            self.current = None;
            self.offset = 0;
        }
        Ok(to_copy)
    }
}

struct RargzSequentialReader<R> {
    reader: R,
    decompressor: Decompressor<'static>,
    chunk_capacity: usize,
    buffer: Vec<u8>,
    offset: usize,
    finished: bool,
}

impl<R: Read> RargzSequentialReader<R> {
    fn new(reader: R, chunk_capacity: usize) -> io::Result<Self> {
        let decompressor =
            Decompressor::new().map_err(|err| io::Error::new(ErrorKind::Other, err.to_string()))?;
        Ok(Self {
            reader,
            decompressor,
            chunk_capacity,
            buffer: Vec::new(),
            offset: 0,
            finished: false,
        })
    }

    fn refill(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }

        let mut len_buf = [0u8; 8];
        if !read_exact_or_eof(&mut self.reader, &mut len_buf)? {
            self.finished = true;
            self.buffer.clear();
            self.offset = 0;
            return Ok(());
        }

        let chunk_len = usize::try_from(u64::from_le_bytes(len_buf))
            .map_err(|_| io::Error::new(ErrorKind::InvalidData, "chunk length exceeds usize"))?;

        let mut compressed = vec![0u8; chunk_len];
        if chunk_len > 0 {
            read_exact_checked(&mut self.reader, &mut compressed)
                .map_err(|err| io::Error::new(err.kind(), format!("failed to read chunk: {err}")))?;
        }

        let data = self
            .decompressor
            .decompress(&compressed, self.chunk_capacity)
            .map_err(|err| io::Error::new(ErrorKind::Other, format!("decompression failed: {err}")))?;
        self.offset = 0;
        self.buffer = data;
        Ok(())
    }
}

impl<R: Read> Read for RargzSequentialReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.offset == self.buffer.len() {
            self.refill()?;
        }

        if self.offset == self.buffer.len() {
            return Ok(0);
        }

        let available = &self.buffer[self.offset..];
        let to_copy = available.len().min(buf.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        self.offset += to_copy;
        Ok(to_copy)
    }
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        let n = reader.read(&mut buf[read..])?;
        if n == 0 {
            return if read == 0 {
                Ok(false)
            } else {
                Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "unexpected EOF while reading chunk header",
                ))
            };
        }
        read += n;
    }
    Ok(true)
}

fn read_exact_checked<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let n = reader.read(&mut buf[read..])?;
        if n == 0 {
            return Err(io::Error::new(
                ErrorKind::UnexpectedEof,
                "unexpected EOF while reading chunk body",
            ));
        }
        read += n;
    }
    Ok(())
}

struct ChunkWriter {
    tx: Sender<Chunk>,
    buffer: Vec<u8>,
    chunk_size: usize,
    next_index: u64,
    abort: AbortSignal,
    progress: ProgressReporter,
}

impl ChunkWriter {
    fn new(
        tx: Sender<Chunk>,
        chunk_size: usize,
        abort: AbortSignal,
        progress: ProgressReporter,
    ) -> Self {
        Self {
            tx,
            buffer: Vec::with_capacity(chunk_size),
            chunk_size,
            next_index: 0,
            abort,
            progress,
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
        self.progress.add_bytes(data.len() as u64);
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
    input
        .file_name()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn default_jobs() -> usize {
    num_cpus::get().max(1)
}
