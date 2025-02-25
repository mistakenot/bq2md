"""BigQuery connection and schema extraction functionality."""

from google.cloud import bigquery
import logging
import os
import json
import random
from genson import SchemaBuilder

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
        
        # Check for JSON fields to sample
        json_fields = []
        
        for field in table.schema:
            field_info = {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description or ""
            }
            
            # Identify JSON fields for sampling
            if field.field_type == "JSON":
                json_fields.append(field.name)
                
            schema_info["fields"].append(field_info)
        
        # Sample JSON fields if any exist
        if json_fields and table.num_rows > 0:
            json_samples = self.sample_json_fields(dataset_id, table_id, json_fields)
            
            # Add JSON schema information to the fields
            for field in schema_info["fields"]:
                if field["name"] in json_samples:
                    field["json_schema"] = json_samples[field["name"]]["schema"]
                    field["json_samples"] = json_samples[field["name"]]["samples"]
        
        logger.info("Retrieved schema for table", {"dataset": dataset_id, "table": table_id})
        return schema_info
    
    def sample_json_fields(self, dataset_id, table_id, json_fields, sample_size=10):
        """Sample JSON fields from a table and infer their schema.
        
        Args:
            dataset_id (str): The BigQuery dataset ID
            table_id (str): The BigQuery table ID
            json_fields (list): List of JSON field names to sample
            sample_size (int, optional): Number of samples to take. Defaults to 10.
            
        Returns:
            dict: Dictionary mapping field names to their JSON schema and samples
        """
        if not json_fields:
            return {}
        
        # Construct query to sample JSON fields
        fields_str = ", ".join(json_fields)
        query = f"""
        SELECT {fields_str}
        FROM `{self.project_id}.{dataset_id}.{table_id}`
        WHERE {" AND ".join([f"{field} IS NOT NULL" for field in json_fields])}
        LIMIT 100
        """
        
        try:
            # Execute query
            query_job = self.client.query(query)
            rows = list(query_job.result())
            
            if not rows:
                logger.warning("No samples found for JSON fields", {
                    "dataset": dataset_id, 
                    "table": table_id, 
                    "fields": json_fields
                })
                return {}
            
            # Randomly select up to sample_size rows
            if len(rows) > sample_size:
                rows = random.sample(rows, sample_size)
            
            result = {}
            
            # Process each JSON field
            for field in json_fields:
                samples = []
                schema_builder = SchemaBuilder()
                
                for row in rows:
                    value = getattr(row, field)
                    if value:
                        try:
                            # Parse JSON if it's a string
                            if isinstance(value, str):
                                parsed = json.loads(value)
                            else:
                                parsed = value
                                
                            # Add to schema builder
                            schema_builder.add_object(parsed)
                            
                            # Add to samples
                            samples.append(parsed)
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.warning(f"Error parsing JSON for field {field}", {
                                "error": str(e),
                                "value": str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                            })
                
                # Generate schema if we have samples
                if samples:
                    result[field] = {
                        "schema": schema_builder.to_schema(),
                        "samples": samples[:3]  # Limit to 3 samples in the output
                    }
            
            logger.info("Sampled JSON fields", {
                "dataset": dataset_id, 
                "table": table_id, 
                "fields": list(result.keys())
            })
            return result
            
        except Exception as e:
            logger.error("Error sampling JSON fields", {
                "dataset": dataset_id, 
                "table": table_id, 
                "error": str(e)
            })
            return {} 