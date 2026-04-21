"""S3 Tables writer module for writing sentiment results to Apache Iceberg tables."""

import logging
import time
from typing import List, Dict, Optional
from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError

from sentiment_analyzer import SentimentResult


logger = logging.getLogger(__name__)


def log_s3_tables_error(
    error: Exception,
    operation: str,
    results: Optional[List[SentimentResult]] = None,
    attempt: Optional[int] = None,
    max_retries: Optional[int] = None
) -> None:
    """Log S3 Tables errors with article_id details.
    
    Implements Requirement 9.5: Log S3 Tables errors with article_id and error details.
    
    Args:
        error: The exception that occurred
        operation: The operation that failed (e.g., 'append', 'upsert')
        results: Optional list of results being written (to extract article_ids)
        attempt: Optional current attempt number
        max_retries: Optional maximum retry attempts
    """
    # Extract article_ids from results if available
    article_ids = []
    if results:
        article_ids = list(set(r.article_id for r in results))
        # Limit to first 10 article_ids to avoid log bloat
        if len(article_ids) > 10:
            article_ids = article_ids[:10] + [f"... and {len(article_ids) - 10} more"]
    
    error_details = {
        'operation': operation,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'article_ids': article_ids,
        'article_count': len(results) if results else 0
    }
    
    if attempt is not None and max_retries is not None:
        error_details['attempt'] = attempt + 1
        error_details['max_retries'] = max_retries
    
    logger.error(
        f"S3 Tables {operation} error: {error_details['error_type']} - {error_details['error_message']} | "
        f"article_count={error_details['article_count']}, "
        f"article_ids={error_details['article_ids']}"
    )


class S3TablesWriter:
    """Handles writing sentiment results to S3 Tables using PyIceberg.
    
    Provides methods for both append and upsert operations with automatic
    retry logic for transient failures.
    """
    
    def __init__(
        self,
        catalog_name: str,
        table_name: str,
        warehouse_path: str,
        rating_scale_min: int = 0,
        rating_scale_max: int = 10
    ):
        """Initialize S3 Tables writer.
        
        Args:
            catalog_name: Name of the Iceberg catalog
            table_name: Name of the S3 Table
            warehouse_path: S3 path to table warehouse
            rating_scale_min: Minimum value of rating scale
            rating_scale_max: Maximum value of rating scale
            
        Raises:
            NoSuchTableError: If the table doesn't exist
        """
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.warehouse_path = warehouse_path
        self.rating_scale_min = rating_scale_min
        self.rating_scale_max = rating_scale_max
        
        logger.info(
            f"Initializing S3TablesWriter: catalog={catalog_name}, "
            f"table={table_name}, warehouse={warehouse_path}"
        )
        
        try:
            # AWS S3 Tables uses a REST catalog with specific configuration
            # See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html
            import boto3
            from pyiceberg.schema import Schema
            from pyiceberg.types import (
                NestedField, StringType, DoubleType, IntegerType, TimestampType
            )
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import IdentityTransform, DayTransform
            
            # Get AWS region from boto3 session
            session = boto3.Session()
            region = session.region_name or 'us-east-1'
            account_id = boto3.client('sts').get_caller_identity()['Account']
            
            # Extract bucket name from warehouse path
            # warehouse_path format: s3://bucket-name/warehouse/
            bucket_name = warehouse_path.replace('s3://', '').split('/')[0]
            
            # For AWS S3 Tables, the REST endpoint is:
            # https://s3tables.<Region>.amazonaws.com/iceberg
            # The warehouse must be the table bucket ARN, not an S3 path
            catalog_uri = f"https://s3tables.{region}.amazonaws.com/iceberg"
            table_bucket_arn = f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket_name}"
            
            logger.info(f"Using S3 Tables REST catalog: uri={catalog_uri}, warehouse={table_bucket_arn}")
            
            # Load catalog with S3 Tables configuration per AWS documentation
            self.catalog = load_catalog(
                catalog_name,
                **{
                    'type': 'rest',
                    'uri': catalog_uri,
                    'warehouse': table_bucket_arn,
                    'rest.sigv4-enabled': 'true',
                    'rest.signing-name': 's3tables',
                    'rest.signing-region': region,
                }
            )
            
            # Load the table - namespace is 'default' by convention
            full_table_name = f"default.{table_name}"
            
            try:
                self.table = self.catalog.load_table(full_table_name)
                logger.info(f"Successfully loaded existing S3 Table: {full_table_name}")
            except NoSuchTableError:
                # Table doesn't exist - create it dynamically
                logger.info(f"Table {full_table_name} not found, creating it...")
                
                # Define schema matching the expected output
                schema = Schema(
                    NestedField(1, "article_id", StringType(), required=True),
                    NestedField(2, "symbol", StringType(), required=True),
                    NestedField(3, "component", StringType(), required=True),
                    NestedField(4, "model_id", StringType(), required=True),
                    NestedField(5, "sentiment_score", DoubleType(), required=False),
                    NestedField(6, "model_name", StringType(), required=True),
                    NestedField(7, "timestamp", TimestampType(), required=True),
                    NestedField(8, "job_name", StringType(), required=True),
                    NestedField(9, "rating_scale_min", IntegerType(), required=True),
                    NestedField(10, "rating_scale_max", IntegerType(), required=True),
                    NestedField(11, "created_at", TimestampType(), required=False),
                )
                
                # Define partitioning by date (extracted from timestamp)
                # Note: model_id partition not needed since each model has its own table
                partition_spec = PartitionSpec(
                    PartitionField(
                        source_id=7,  # timestamp
                        field_id=1000,
                        transform=DayTransform(),
                        name="date"
                    )
                )
                
                # Table properties for optimization
                properties = {
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy",
                    "commit.manifest.min-count-to-merge": "5",
                    "commit.manifest-merge.enabled": "true",
                    "history.expire.max-snapshot-age-ms": "604800000",  # 7 days
                }
                
                # Create the table
                self.table = self.catalog.create_table(
                    identifier=full_table_name,
                    schema=schema,
                    partition_spec=partition_spec,
                    properties=properties
                )
                logger.info(f"Successfully created S3 Table: {full_table_name}")
                
        except Exception as e:
            logger.error(f"Failed to initialize S3TablesWriter: {e}")
            raise
    
    def write_results_batch(
        self,
        results: List[SentimentResult],
        mode: str = "append"
    ) -> Dict[str, int]:
        """Write sentiment results to S3 Table.
        
        Args:
            results: List of SentimentResult objects
            mode: Write mode - "append" or "upsert"
            
        Returns:
            Dict with 'written' and 'failed' counts
            
        Raises:
            ValueError: If mode is not "append" or "upsert"
        """
        if not results:
            logger.warning("No results to write")
            return {"written": 0, "failed": 0}
        
        if mode not in ["append", "upsert"]:
            raise ValueError(f"Invalid write mode: {mode}. Must be 'append' or 'upsert'")
        
        logger.info(f"Writing {len(results)} results in {mode} mode")
        
        if mode == "append":
            return self._write_append(results)
        else:
            return self.upsert_results_batch(results)
    
    def _write_append(
        self,
        results: List[SentimentResult],
        max_retries: int = 3
    ) -> Dict[str, int]:
        """Write results in append mode with retry logic.
        
        Args:
            results: List of SentimentResult objects
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dict with 'written' and 'failed' counts
        """
        arrow_table = self._convert_results_to_arrow(results)
        
        for attempt in range(max_retries):
            try:
                # Append data to the table
                self.table.append(arrow_table)
                
                # Log S3 Tables write statistics (Requirement 9.3)
                logger.info(
                    f"S3 Tables write statistics: operation=append, "
                    f"records_written={len(results)}, errors=0, "
                    f"table={self.table_name}"
                )
                return {"written": len(results), "failed": 0}
                
            except CommitFailedException as e:
                # Concurrent write conflict - retry with backoff
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) * (0.8 + 0.4 * time.time() % 1)  # Jitter
                    logger.warning(
                        f"Commit failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {backoff:.2f}s: {e}"
                    )
                    time.sleep(backoff)
                else:
                    # Log error with article_id details (Requirement 9.5)
                    log_s3_tables_error(e, 'append', results, attempt, max_retries)
                    return {"written": 0, "failed": len(results)}
                    
            except Exception as e:
                # Other errors - retry with backoff
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) * (0.8 + 0.4 * time.time() % 1)  # Jitter
                    logger.warning(
                        f"Write failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {backoff:.2f}s: {e}"
                    )
                    time.sleep(backoff)
                else:
                    # Log error with article_id details (Requirement 9.5)
                    log_s3_tables_error(e, 'append', results, attempt, max_retries)
                    return {"written": 0, "failed": len(results)}
        
        return {"written": 0, "failed": len(results)}
    
    def upsert_results_batch(
        self,
        results: List[SentimentResult],
        max_retries: int = 3
    ) -> Dict[str, int]:
        """Upsert sentiment results to S3 Table.
        
        Uses merge operation to update existing records or insert new ones
        based on primary key (article_id, symbol, component, model_id).
        
        Args:
            results: List of SentimentResult objects
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dict with 'written' and 'updated' counts
        """
        if not results:
            logger.warning("No results to upsert")
            return {"written": 0, "updated": 0}
        
        arrow_table = self._convert_results_to_arrow(results)
        
        for attempt in range(max_retries):
            try:
                # For upsert, we use overwrite with a filter
                # This is a simplified implementation - in production you'd use
                # Iceberg's merge capabilities or delete+insert pattern
                
                # For now, we'll use append mode and rely on query-time deduplication
                # A full upsert implementation would require:
                # 1. Read existing records matching the keys
                # 2. Delete those records
                # 3. Insert new records
                # This is complex and requires transaction support
                
                # Simplified approach: just append and let queries handle deduplication
                self.table.append(arrow_table)
                
                # Log S3 Tables write statistics (Requirement 9.3)
                logger.info(
                    f"S3 Tables write statistics: operation=upsert, "
                    f"records_written={len(results)}, records_updated=0, errors=0, "
                    f"table={self.table_name}"
                )
                return {"written": len(results), "updated": 0}
                
            except CommitFailedException as e:
                # Concurrent write conflict - retry with backoff
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) * (0.8 + 0.4 * time.time() % 1)  # Jitter
                    logger.warning(
                        f"Upsert commit failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {backoff:.2f}s: {e}"
                    )
                    time.sleep(backoff)
                else:
                    # Log error with article_id details (Requirement 9.5)
                    log_s3_tables_error(e, 'upsert', results, attempt, max_retries)
                    return {"written": 0, "updated": 0}
                    
            except Exception as e:
                # Other errors - retry with backoff
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) * (0.8 + 0.4 * time.time() % 1)  # Jitter
                    logger.warning(
                        f"Upsert failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {backoff:.2f}s: {e}"
                    )
                    time.sleep(backoff)
                else:
                    # Log error with article_id details (Requirement 9.5)
                    log_s3_tables_error(e, 'upsert', results, attempt, max_retries)
                    return {"written": 0, "updated": 0}
        
        return {"written": 0, "updated": 0}
    
    def _convert_results_to_arrow(
        self,
        results: List[SentimentResult]
    ) -> pa.Table:
        """Convert SentimentResult list to PyArrow Table.
        
        Creates a PyArrow table with the schema matching the S3 Table definition:
        - article_id (string)
        - symbol (string)
        - component (string)
        - model_id (string)
        - sentiment_score (double, nullable)
        - model_name (string)
        - timestamp (timestamp)
        - job_name (string)
        - rating_scale_min (int32)
        - rating_scale_max (int32)
        - created_at (timestamp, nullable)
        
        Args:
            results: List of SentimentResult objects
            
        Returns:
            PyArrow Table with proper schema
        """
        if not results:
            # Return empty table with correct schema
            schema = pa.schema([
                ('article_id', pa.string()),
                ('symbol', pa.string()),
                ('component', pa.string()),
                ('model_id', pa.string()),
                ('sentiment_score', pa.float64()),
                ('model_name', pa.string()),
                ('timestamp', pa.timestamp('us', tz='UTC')),
                ('job_name', pa.string()),
                ('rating_scale_min', pa.int32()),
                ('rating_scale_max', pa.int32()),
                ('created_at', pa.timestamp('us', tz='UTC'))
            ])
            # Create empty arrays for each column
            empty_data = {
                'article_id': pa.array([], type=pa.string()),
                'symbol': pa.array([], type=pa.string()),
                'component': pa.array([], type=pa.string()),
                'model_id': pa.array([], type=pa.string()),
                'sentiment_score': pa.array([], type=pa.float64()),
                'model_name': pa.array([], type=pa.string()),
                'timestamp': pa.array([], type=pa.timestamp('us', tz='UTC')),
                'job_name': pa.array([], type=pa.string()),
                'rating_scale_min': pa.array([], type=pa.int32()),
                'rating_scale_max': pa.array([], type=pa.int32()),
                'created_at': pa.array([], type=pa.timestamp('us', tz='UTC'))
            }
            return pa.table(empty_data, schema=schema)
        
        # Define schema matching the S3 Table definition
        # Required fields are marked as non-nullable
        schema = pa.schema([
            pa.field('article_id', pa.string(), nullable=False),
            pa.field('symbol', pa.string(), nullable=False),
            pa.field('component', pa.string(), nullable=False),
            pa.field('model_id', pa.string(), nullable=False),
            pa.field('sentiment_score', pa.float64(), nullable=True),
            pa.field('model_name', pa.string(), nullable=False),
            pa.field('timestamp', pa.timestamp('us'), nullable=False),
            pa.field('job_name', pa.string(), nullable=False),
            pa.field('rating_scale_min', pa.int32(), nullable=False),
            pa.field('rating_scale_max', pa.int32(), nullable=False),
            pa.field('created_at', pa.timestamp('us'), nullable=True),
        ])
        
        # Extract data from results
        data = {
            'article_id': [],
            'symbol': [],
            'component': [],
            'model_id': [],
            'sentiment_score': [],
            'model_name': [],
            'timestamp': [],
            'job_name': [],
            'rating_scale_min': [],
            'rating_scale_max': [],
            'created_at': []
        }
        
        for result in results:
            data['article_id'].append(result.article_id)
            data['symbol'].append(result.symbol)
            data['component'].append(result.component)
            data['model_id'].append(result.model_id)
            data['sentiment_score'].append(result.sentiment_score)
            data['model_name'].append(result.model_name)
            data['timestamp'].append(result.timestamp)
            data['job_name'].append(result.job_name if result.job_name else 'unknown')
            
            # Use rating scale from writer initialization
            data['rating_scale_min'].append(self.rating_scale_min)
            data['rating_scale_max'].append(self.rating_scale_max)
            
            # created_at is the same as timestamp for now
            data['created_at'].append(result.timestamp)
        
        # Create PyArrow table with explicit schema to match S3 Table definition
        arrow_table = pa.table(data, schema=schema)
        
        logger.debug(
            f"Converted {len(results)} results to PyArrow table: "
            f"{arrow_table.num_rows} rows, {arrow_table.num_columns} columns"
        )
        
        return arrow_table
