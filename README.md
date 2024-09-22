# `<placeholder>` 🚀

`<placeholder>` is a command-line tool built on **DuckDB** to provide lightning-fast inspection and validation of Apache Parquet files. It's simple, powerful, and gets the job done without unnecessary complexity. 🎯 Whether you need to check metadata, inspect records, or verify file integrity, `<placeholder>` offers everything you need in a flash.

## ⚡ Features
- 🔍 **Meta**: Extract basic file metadata without loading the full schema.
- 🔢 **Head**: Quickly display the first few records of your file.
- 📊 **Stats**: Get min, max, count, and other useful statistics for columns.
- 📝 **Cat**: Concatenate and print all records to stdout.
- ✅ **Verify**: Ensure the file is valid by checking checksums, magic bytes, and corruption.
- 🛠️ **Schema**: Inspect the file’s schema in detail.

Powered by **DuckDB**, this tool is designed for performance and reliability when working with Parquet files.

## Installation

You can install `<placeholder>` using your preferred method:

```bash
pip install placeholder-cli  # For Python-based installation
# or
brew install placeholder  # For macOS Homebrew users
```

Usage

```bash
pli --help                                                                                                    
                                                                                                                       
 Usage: pli [OPTIONS] COMMAND [ARGS]...                                                                                
                                                                                                                       
╭─ Options ───────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                                             │
│ --show-completion             Show completion for the current shell, to copy it or customize the installation.      │
│ --help                        Show this message and exit.                                                           │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ cat      Concatenate and display all records in the Parquet file.                                                   │
│ head     Display the first N records from the Parquet file.                                                         │
│ meta     Display basic metadata of the Parquet file without loading the schema.                                     │
│ schema   Display the schema of the Parquet file.                                                                    │
│ stats    Display statistics (min, max, count, etc.) for columns in the Parquet file.                                │
│ verify   Verify the integrity of the Parquet file by checking checksums, magic bytes, and corruption.               │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
```