# `pli` 🚀
pli /pli/ (noun, sv):
1. Strict, disciplined behavior, often instilled or imposed.

   _få pli på något_ — to get something under control.


`pli`  is your Parquet CLI. Built with DuckDB to provide fast and easy inspection and validation of Apache Parquet files

## ⚡ Features
- 🔍 **Meta**: Extract basic file metadata without loading the full schema.
- 🔢 **Head**: Quickly display the first few records of your file.
- 📊 **Stats**: Get min, max, count, and other useful statistics for columns.
- 📝 **Cat**: Concatenate and print all records to stdout.
- ✅ **Verify**: Ensure the file is valid by checking checksums, magic bytes, and corruption.
- 🛠️ **Schema**: Inspect the file’s schema in detail.

Powered by **DuckDB**, this tool is designed for performance and reliability when working with Parquet files.

## Installation

You can install `pli` using your preferred method:

```bash
pip install pli-cli  # For Python-based installation
# or
brew install pli  # For macOS Homebrew users
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


# todo
- duckdb
- tests
- remote repo
- fix name
- pypi, brew
- license
- think about cutting edge features, as in smooth investigation on the presence of statistics, selectivity, overlap between rowgroups, etc

