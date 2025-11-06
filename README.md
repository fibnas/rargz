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
- Opt-in `.rargz` chunked format with cooperative parallel decompression
- Streaming extraction (`stdin` to filesystem) with automatic format detection
- Metadata inspection via `--list` and `--count`

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

Tune chunk size and thread count:

```bash
rargz --chunk-size 4MiB --jobs 8 path/to/input > archive.tar.zst
```

Disable the progress spinner explicitly when scripting:

```bash
rargz --no-progress path/to/input > archive.tar.zst
```

## Extraction

Extraction reads from stdin (or a path argument) and writes the output to the directory specified with `-o/--output`:

```bash
rargz --extract archive.tar.zst -o ./output
```

Or stream over stdin:

```bash
rargz --extract -o /path/to/output < archive.tar.zst
```
or

```bash
cat archive.tar.zst | rargz --extract -o /path/to/output
```

`rargz` auto-detects plain `.tar.zst` streams versus chunked `.rargz` streams. Standard archives are decompressed sequentially for compatibility. Chunked archives fan out to the worker pool for parallel decompression and untar.

Progress is enabled by default; disable it with `--no-progress` or by piping stderr.

## Inspecting archives

Scan metadata without unpacking by either piping the archive or passing it as an argument:

```bash
rargz --list archive.tar.zst
rargz --count archive.rargz
```

`--list` prints each entry path (similar to `tar -tf`), while `--count` emits a single total. Both modes skip extracting payloads and work with either format. When no path is supplied, the commands read from stdin (so you can still pipe data in).

## Format details

- `.tar.zst` mode produces a pure tar stream compressed with zstd. Fully compatible with standard tools:
  - `tar --zstd -xf archive.tar.zst`
  - `zstd -d archive.tar.zst | tar -xf -`

- `.rargz` mode adds a small header (`RARGZ\0`, version, chunk size) and length-prefixes each compressed chunk. The extra framing allows `rargz` to decompress different chunks in parallel during extraction or inspection.

## Safety

- Parent directory components (`..`) are removed from archive paths to prevent extraction from writing outside the target tree.
- Any error in the pipeline cancels all worker threads immediately.

## License

MIT. See `LICENSE` for details.
