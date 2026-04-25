# Parallel Upload Test Bundle

Tests parallel upload performance with many small files (200 x 100KB = 20MB).

## Setup

Generate test input files before running (not committed to git):

```bash
cd inputs
for i in $(seq 1 200); do dd if=/dev/urandom of=file_$i.bin bs=100000 count=1 2>/dev/null; done
```

## Usage

```bash
# Time Python vs Rust (use fresh files not in S3 cache)
time deadline bundle submit . --yes --queue-id QUEUE 2>&1
time ./target/debug/deadline bundle submit . --yes --queue-id QUEUE 2>&1
```
