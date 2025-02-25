"""BigQuery connection and schema extraction functionality."""

from google.cloud import bigquery
import logging
import os

logger = logging.getLogger(__name__)

class BigQueryClient:
    """Client for interacting with BigQuery and extracting schema information."""
    
    def __init__(self, project_id=None):
        """Initialize the BigQuery client.
        
        Args:
            project_id (str, optional): The Google Cloud project ID. Defaults to None.
        """
        # Use the provided project_id, or get from environment, or default to 'ateams'
        self.project_id = project_id or os.getenv("PROJECT_ID") or "ateams"
        self.client = bigquery.Client(project=self.project_id)
        logger.info("BigQuery client initialized", {"project": self.project_id})
    
    def get_dataset_tables(self, dataset_id):
        """Get a list of all tables in the specified dataset.
        
        Args:
            dataset_id (str): The BigQuery dataset ID
            
        Returns:
            list: List of table references
        """
        dataset_ref = self.client.dataset(dataset_id)
        tables = list(self.client.list_tables(dataset_ref))
        
        logger.info("Retrieved tables from dataset", {"dataset": dataset_id, "table_count": len(tables)})
        return tables
    
    def get_table_schema(self, dataset_id, table_id):
        """Get the schema for a specific table.
        
        Args:
            dataset_id (str): The BigQuery dataset ID
            table_id (str): The BigQuery table ID
            
        Returns:
            dict: Table schema information
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = self.client.get_table(table_ref)
        
        schema_info = {
            "name": table.table_id,
            "description": table.description or "",
            "num_rows": table.num_rows,
            "created": table.created.isoformat() if table.created else "",
            "fields": []
        }
        
        for field in table.schema:
            field_info = {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description or ""
            }
            schema_info["fields"].append(field_info)
        
        logger.info("Retrieved schema for table", {"dataset": dataset_id, "table": table_id})
        return schema_info 