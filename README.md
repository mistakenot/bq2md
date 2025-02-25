# BigQuery 2 Markdown (bq2md)

A simple CLI tool to extract BigQuery table schemas and convert them to Markdown format, making them easily accessible for both humans and LLMs.

## Installation

```bash
# Clone the repository
git clone https://github.com/mistakenot/bq2md.git
cd bq2md

# Install with Poetry
poetry install
```

## Usage

```bash
# Extract schema from a BigQuery dataset and save to a markdown file
bq2md --dataset your_dataset_name ./output_schema.md
```

## Authentication

The tool supports multiple authentication methods for Google Cloud:

1. **Service Account Key File** (recommended for production):
   - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file:
   ```
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json
   ```
   - Or add it to your `.env` file

2. **Application Default Credentials**:
   - If you've already authenticated with `gcloud auth application-default login`, the tool will automatically use these credentials
   - The tool checks for credentials at `~/.config/gcloud/application_default_credentials.json`

3. **Project ID**:
   - By default, the tool uses the project ID from your credentials
   - You can override this by setting the `PROJECT_ID` environment variable

## Schema Extraction Features

The tool extracts comprehensive schema information:

- **Basic Table Information**:
  - Table name and description
  - Row count and creation date
  - Complete field listing with types, modes, and descriptions

- **JSON Field Analysis**:
  - Automatic detection of JSON fields in tables
  - Schema inference from JSON field samples
  - Sample JSON values included in the output (up to 3 samples)
  - Uses the `genson` library to generate JSON schema from samples

- **Markdown Output**:
  - Clean, well-formatted markdown tables
  - Hierarchical structure with dataset and table sections
  - Detailed field information
  - Collapsible sections for JSON schema details

## Requirements

- Python 3.13+
- Google Cloud credentials with BigQuery access
- Access to the BigQuery datasets you want to document

## License

MIT
