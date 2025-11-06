# rargz

[![Crates.io](https://img.shields.io/crates/v/rargz.svg)](https://crates.io/crates/rargz)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/Rust-stable-orange.svg)](https://www.rust-lang.org)

`rargz` is a fast parallel tar + zstd archiver. It walks a directory, emits a deterministic tar stream, and compresses chunks concurrently with independent zstd workers.

> 671,352 files → 27 seconds:

<img width="805" height="379" alt="rargz" src="https://github.com/user-attachments/assets/27be47b0-d7d2-4b0b-b38f-82c49104c397" />

## Features

- Parallel tar stream generation with independent zstd workers
- Optional progress indicator (auto-disables when stderr is not a TTY)
- Produces standard `.tar.zst` by default (compatible with `tar --zstd` or `zstd -d`)
- Optional `.rargz` chunked format (designed for future parallel decompression)
- Streaming extraction (`stdin` to filesystem)

<img width="1016" height="488" alt="rargz-h" src="https://github.com/user-attachments/assets/d20c01d3-ded6-4243-80f1-c53f22d2f205" />

<img width="1016" height="488" alt="rargz-h" src="https://github.com/user-attachments/assets/d20c01d3-ded6-4243-80f1-c53f22d2f205" />

## Installation

```bash
cargo install rargz
```

From source:

```bash
cargo install --path .
```

## Archiving

Create a standard `.tar.zst` archive and write it to stdout:

```bash
rargz path/to/input > archive.tar.zst
```

Enable the chunked `.rargz` format:

```bash
rargz --format rargz path/to/input > archive.rargz
```

Tune chunk size (bytes) and thread count:

```bash
rargz --chunk-size 4194304 --jobs 8 path/to/input > archive.tar.zst
```

Raise the compression level (zstd 1–22, default 3) and silence the progress spinner:

```bash
rargz --level 9 --no-progress path/to/input > archive.tar.zst
```

## Extraction

Extraction reads from stdin and writes the output to the directory specified with `-o/--output`:

```bash
rargz --extract -o /path/to/output < archive.tar.zst
```
or

```bash
rargz --extract -o /path/to/output < archive.tar.zst
```
or

```bash
cat archive.tar.zst | rargz --extract -o /path/to/output
```

`.tar.zst` streams are decompressed sequentially for compatibility.  
`.rargz` streams are chunked and designed for parallel decompression (WIP until format is finalized).

You can also extract directly from a file without an explicit pipe:

```bash
rargz --extract -o /path/to/output archive.tar.zst
```

## Format details

- `.tar.zst` mode produces a pure tar stream compressed with zstd. Fully compatible with standard tools:
  - `tar --zstd -xf archive.tar.zst`
  - `zstd -d archive.tar.zst | tar -xf -`

- `.rargz` mode adds a small header (`RARGZ\0`, version, chunk size) and length-prefixes each compressed chunk. This enables random access and parallel decompression.

## Inspection

Read-only operations work on both `.tar.zst` and `.rargz` streams supplied via a file or stdin.

- `--list` prints every entry path:

  ```bash
  rargz --list archive.tar.zst
  # or
  rargz --list < archive.tar.zst
  ```

- `--count` reports how many entries are present:

  ```bash
  rargz --count archive.tar.zst
  ```

Both modes consume the entire archive to validate it; they exit non-zero on structural errors.

## Performance notes

- `--jobs` controls the compression worker pool (defaults to the number of logical CPUs).
- `--chunk-size` specifies the amount of tar data each compressor sees at once; larger chunks improve ratio, smaller chunks reduce memory per worker.
- `--format rargz` emits independently compressed chunks with a lightweight header for future parallel extraction support.
- `--no-progress` disables the progress indicator (also auto-disables when stderr is not a TTY).

## Safety

- Parent directory components (`..`) are removed from archive paths to prevent extraction from writing outside the target tree.
- Any error in the pipeline cancels all worker threads immediately.

## License

MIT. See `LICENSE` for details.
