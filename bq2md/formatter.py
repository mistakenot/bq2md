"""Format BigQuery schema information as Markdown."""

import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class MarkdownFormatter:
    """Format BigQuery schema information as Markdown."""
    
    @staticmethod
    def format_table_schema(schema_info):
        """Format a single table schema as Markdown.
        
        Args:
            schema_info (dict): Table schema information
            
        Returns:
            str: Markdown formatted schema
        """
        md_lines = []
        
        # Table header
        md_lines.append(f"# Table: {schema_info['name']}")
        md_lines.append("")
        
        # Table description
        if schema_info['description']:
            md_lines.append(schema_info['description'])
            md_lines.append("")
        
        # Table metadata
        md_lines.append(f"**Rows**: {schema_info['num_rows']:,}")
        if schema_info['created']:
            md_lines.append(f"**Created**: {schema_info['created']}")
        md_lines.append("")
        
        # Fields header
        md_lines.append("## Schema")
        md_lines.append("")
        md_lines.append("| Field | Type | Mode | Description |")
        md_lines.append("|-------|------|------|-------------|")
        
        # Fields
        json_fields = []
        for field in schema_info['fields']:
            name = field['name']
            field_type = field['type']
            mode = field['mode']
            description = field['description'].replace("\n", " ")
            
            md_lines.append(f"| {name} | {field_type} | {mode} | {description} |")
            
            # Track JSON fields for detailed display later
            if field_type == "JSON" and "json_schema" in field:
                json_fields.append(field)
        
        md_lines.append("")
        
        # Add detailed JSON field information if available
        for field in json_fields:
            md_lines.append(f"### JSON Field: {field['name']}")
            md_lines.append("")
            
            # Add JSON schema
            md_lines.append("#### Schema")
            md_lines.append("```json")
            md_lines.append(json.dumps(field['json_schema'], indent=2))
            md_lines.append("```")
            md_lines.append("")
            
            # Add sample values
            if "json_samples" in field and field["json_samples"]:
                md_lines.append("#### Sample Values")
                for i, sample in enumerate(field["json_samples"], 1):
                    md_lines.append(f"**Sample {i}:**")
                    md_lines.append("```json")
                    md_lines.append(json.dumps(sample, indent=2))
                    md_lines.append("```")
                    md_lines.append("")
        
        return "\n".join(md_lines)
    
    @staticmethod
    def format_dataset_schemas(dataset_id, schemas):
        """Format all schemas in a dataset as a single Markdown document.
        
        Args:
            dataset_id (str): The BigQuery dataset ID
            schemas (list): List of schema information dictionaries
            
        Returns:
            str: Markdown formatted schemas
        """
        md_lines = []
        
        # Dataset header
        md_lines.append(f"# Dataset: {dataset_id}")
        md_lines.append("")
        md_lines.append(f"This document contains the schema information for {len(schemas)} tables in the `{dataset_id}` dataset.")
        md_lines.append("")
        
        # Table of contents
        md_lines.append("## Tables")
        md_lines.append("")
        
        for schema in schemas:
            md_lines.append(f"- [{schema['name']}](#{schema['name'].lower()})")
        
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")
        
        # Add each table schema
        for schema in schemas:
            table_md = MarkdownFormatter.format_table_schema(schema)
            md_lines.append(f"<a id='{schema['name'].lower()}'></a>")
            md_lines.append(table_md)
            md_lines.append("---")
            md_lines.append("")
        
        return "\n".join(md_lines)
    
    @staticmethod
    def save_markdown(markdown_content, output_path):
        """Save markdown content to a file.
        
        Args:
            markdown_content (str): The markdown content to save
            output_path (str): Path to save the markdown file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            output_file = Path(output_path)
            output_file.write_text(markdown_content)
            logger.info("Saved markdown file", {"path": str(output_file)})
            return True
        except Exception as e:
            logger.error("Failed to save markdown file", {"path": output_path, "error": str(e)})
            return False 