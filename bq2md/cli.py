"""Command-line interface for bq2md."""

import sys
import click
import logging
from pathlib import Path

from bq2md.config import check_credentials
from bq2md.bigquery import BigQueryClient
from bq2md.formatter import MarkdownFormatter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@click.command()
@click.option(
    "--dataset",
    required=True,
    help="BigQuery dataset ID to extract schema from"
)
@click.argument(
    "output_file",
    type=click.Path(dir_okay=False)
)
def main(dataset, output_file):
    """Extract BigQuery table schemas and save as Markdown.
    
    OUTPUT_FILE is the path where the Markdown file will be saved.
    """
    # Check credentials
    creds_ok, creds_msg = check_credentials()
    if not creds_ok:
        logger.error("Credentials error", {"error": creds_msg})
        click.echo(f"Error: {creds_msg}", err=True)
        click.echo("Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable.", err=True)
        sys.exit(1)
    
    try:
        # Initialize BigQuery client
        bq_client = BigQueryClient()
        
        # Get tables in dataset
        tables = bq_client.get_dataset_tables(dataset)
        
        if not tables:
            logger.warning("No tables found in dataset", {"dataset": dataset})
            click.echo(f"Warning: No tables found in dataset '{dataset}'")
            sys.exit(0)
        
        click.echo(f"Found {len(tables)} tables in dataset '{dataset}'")
        
        # Extract schema for each table
        schemas = []
        with click.progressbar(tables, label="Extracting schemas") as progress_tables:
            for table in progress_tables:
                schema = bq_client.get_table_schema(dataset, table.table_id)
                schemas.append(schema)
        
        # Format schemas as markdown
        click.echo("Formatting as Markdown...")
        markdown_content = MarkdownFormatter.format_dataset_schemas(dataset, schemas)
        
        # Save to file
        output_path = Path(output_file)
        MarkdownFormatter.save_markdown(markdown_content, output_path)
        
        click.echo(f"Successfully saved schema to {output_path}")
        logger.info("Command completed successfully", {"dataset": dataset, "output": str(output_path)})
        
    except Exception as e:
        logger.error("Command failed", {"error": str(e)})
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 