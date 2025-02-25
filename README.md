# BigQuery 2 Markdown (bq2md)

A simple CLI tool to extract BigQuery table schemas and convert them to Markdown format.

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/bq2md.git
cd bq2md

# Install with Poetry
poetry install
```

## Usage

```bash
# Extract schema from a BigQuery dataset and save to a markdown file
bq2md --dataset your_dataset_name ./output_schema.md
```

## Requirements

- Python 3.13+
- Google Cloud credentials with BigQuery access

## Configuration

Create a `.env` file with your Google Cloud credentials:

```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json
```

## License

MIT
