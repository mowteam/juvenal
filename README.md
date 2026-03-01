# juvenal

`juvenal` is a deterministic Rust CLI that generates configurable greetings in text or JSON.

## Features

- Configurable `name`, `language`, `style`, `punctuation`, and `intensity`
- Output formats: plain text (`--output text`) or JSON (`--output json`)
- Optional UNIX timestamp injection with `--timestamp`
- Input normalization (whitespace collapse, default name fallback)
- Strict validation and typed error messages
- Stable exit code contract for automation

## Build and Install

```bash
# Build debug binary
cargo build

# Build optimized binary
cargo build --release

# Optional: install locally
cargo install --path .
```

## CLI Usage

```bash
juvenal [OPTIONS]
```

Options:

- `--name <NAME>` (default: `World`)
- `--language <english|spanish|french>` (default: `english`)
- `--style <casual|formal|excited>` (default: `casual`)
- `--punctuation <auto|none|period|exclamation>` (default: `auto`)
- `--intensity <0..=5>` (default: `1`)
- `--output <text|json>` (default: `text`)
- `--timestamp` (include `timestamp_unix_seconds`)
- `-h, --help`
- `-V, --version`

## CLI Examples

Default output:

```bash
$ juvenal
Hello, World!!
```

JSON output:

```bash
$ juvenal --output json
{"message":"Hello, World!!"}
```

Custom greeting:

```bash
$ juvenal --name "  Ada   Lovelace  " --language french --style excited --punctuation exclamation --intensity 2
BONJOUR, ADA LOVELACE!!!
```

JSON with timestamp:

```bash
$ juvenal --output json --timestamp
{"message":"Hello, World!!","timestamp_unix_seconds":1700000123}
```

`timestamp_unix_seconds` is generated at runtime and will vary.

Validation error (user input error, exit code `2`):

```bash
$ juvenal --style formal --punctuation exclamation --intensity 4
formal style cannot be combined with exclamation punctuation above intensity 3 (received 4)
```

## Output and Exit Code Contracts

- Text mode prints exactly one line to `stdout`, newline-terminated.
- JSON mode prints exactly one JSON object per line to `stdout`.
- JSON always includes:
  - `message: string`
- JSON includes `timestamp_unix_seconds: u64` only when `--timestamp` is set.
- Validation and parsing errors print to `stderr` and return exit code `2`.
- Runtime errors (I/O, time, serialization) return exit code `1`.
- `--help` and `--version` return exit code `0`.

## Verification Commands

Use these before merging:

```bash
# Format check
cargo fmt -- --check

# Full test suite
cargo test

# High-volume matrix coverage
cargo test --test output_matrix

# Property-based invariants (fixed-seed, bounded cases)
cargo test --test properties
```

Optional lint gate (if clippy is installed):

```bash
cargo clippy --all-targets --all-features -- -D warnings
```
