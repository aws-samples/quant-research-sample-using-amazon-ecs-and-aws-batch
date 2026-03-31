"""S3 operations module for reading articles and writing results."""

import asyncio
import json
import logging
import random
import time
from typing import Iterator, Dict, List, Optional
from io import BytesIO
import pandas as pd
import boto3
import aioboto3
from botocore.exceptions import ClientError, NoCredentialsError


logger = logging.getLogger(__name__)


class S3Client:
    """Client for handling S3 read and write operations."""
    
    def __init__(self, boto3_session=None):
        """Initialize S3 client with boto3 session.
        
        Args:
            boto3_session: Optional boto3 Session. If None, uses default session.
        """
        if boto3_session is None:
            boto3_session = boto3.Session()
        self.s3_client = boto3_session.client('s3')
        logger.info("S3Client initialized")
    
    def read_articles(self, bucket_uri: str) -> Iterator[Dict]:
        """Stream articles from S3 bucket as JSON objects.
        
        Args:
            bucket_uri: S3 URI in format 's3://bucket-name/prefix/' or 's3://bucket-name'
            
        Yields:
            Dict: Parsed JSON article objects
            
        Raises:
            ValueError: If bucket_uri format is invalid
            ClientError: If S3 access fails (bucket not found, permission denied)
        """
        bucket_name, prefix = self._parse_s3_uri(bucket_uri)
        logger.info(f"Reading articles from s3://{bucket_name}/{prefix}")
        
        try:
            # List objects in the bucket with the given prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            article_count = 0
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # Skip directories and non-JSON files
                    if key.endswith('/') or not key.endswith('.json'):
                        continue
                    
                    try:
                        # Get object from S3
                        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                        content = response['Body'].read().decode('utf-8')
                        
                        # Parse JSON
                        article = json.loads(content)
                        article_count += 1
                        yield article
                        
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON from {key}: {e}")
                        continue
                    except ClientError as e:
                        logger.error(f"Failed to read object {key}: {e}")
                        continue
            
            logger.info(f"Successfully read {article_count} articles from S3")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchBucket', 'Message': f'Bucket {bucket_name} does not exist'}},
                    'ListObjectsV2'
                )
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to bucket: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'AccessDenied', 'Message': f'Permission denied for bucket {bucket_name}'}},
                    'ListObjectsV2'
                )
            else:
                logger.error(f"S3 error while reading articles: {e}")
                raise
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise

    def download_file(self, s3_uri: str, local_path: str) -> None:
        """Download a file from S3 to local filesystem.
        
        Args:
            s3_uri: S3 URI in format 's3://bucket-name/path/to/file'
            local_path: Local filesystem path where file should be saved
            
        Raises:
            ValueError: If s3_uri format is invalid
            ClientError: If S3 access fails (NoSuchKey, NoSuchBucket, AccessDenied)
        """
        bucket_name, key = self._parse_s3_uri(s3_uri)
        logger.info(f"Downloading file from s3://{bucket_name}/{key} to {local_path}")
        
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read()
            
            # Write to local file
            with open(local_path, 'wb') as f:
                f.write(content)
            
            logger.info(f"Successfully downloaded file to {local_path} ({len(content)} bytes)")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                logger.error(f"File not found: s3://{bucket_name}/{key}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchKey', 'Message': f'File not found at s3://{bucket_name}/{key}'}},
                    'GetObject'
                )
            elif error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchBucket', 'Message': f'Bucket {bucket_name} does not exist'}},
                    'GetObject'
                )
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to s3://{bucket_name}/{key}")
                raise ClientError(
                    {'Error': {'Code': 'AccessDenied', 'Message': f'Permission denied for s3://{bucket_name}/{key}'}},
                    'GetObject'
                )
            else:
                logger.error(f"S3 error while downloading file: {e}")
                raise
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise

    def read_prompt_template(self, s3_uri: str) -> str:
        """Read prompt template from S3.
        
        Args:
            s3_uri: S3 URI in format 's3://bucket-name/path/to/prompt.txt'
            
        Returns:
            str: Content of the prompt template
            
        Raises:
            ValueError: If s3_uri format is invalid
            ClientError: If S3 access fails (object not found, permission denied)
        """
        bucket_name, key = self._parse_s3_uri(s3_uri)
        logger.info(f"Reading prompt template from s3://{bucket_name}/{key}")
        
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            logger.info(f"Successfully read prompt template ({len(content)} characters)")
            return content
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                logger.error(f"Prompt template not found: s3://{bucket_name}/{key}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchKey', 'Message': f'Prompt template not found at s3://{bucket_name}/{key}'}},
                    'GetObject'
                )
            elif error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchBucket', 'Message': f'Bucket {bucket_name} does not exist'}},
                    'GetObject'
                )
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to s3://{bucket_name}/{key}")
                raise ClientError(
                    {'Error': {'Code': 'AccessDenied', 'Message': f'Permission denied for s3://{bucket_name}/{key}'}},
                    'GetObject'
                )
            else:
                logger.error(f"S3 error while reading prompt template: {e}")
                raise
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
    
    def write_parquet(self, df: pd.DataFrame, bucket_uri: str, filename: str) -> None:
        """Write DataFrame to S3 as Parquet format.
        
        Args:
            df: pandas DataFrame to write
            bucket_uri: S3 URI in format 's3://bucket-name/prefix/' or 's3://bucket-name'
            filename: Name of the output file (e.g., 'results_model1.parquet')
            
        Raises:
            ValueError: If bucket_uri format is invalid
            ClientError: If S3 write fails (permission denied, bucket not found)
        """
        bucket_name, prefix = self._parse_s3_uri(bucket_uri)
        
        # Construct full key path
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        key = f"{prefix}{filename}"
        
        logger.info(f"Writing Parquet file to s3://{bucket_name}/{key} ({len(df)} rows)")
        
        try:
            # Write DataFrame to Parquet in memory
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            logger.info(f"Successfully wrote Parquet file to s3://{bucket_name}/{key}")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchBucket', 'Message': f'Bucket {bucket_name} does not exist'}},
                    'PutObject'
                )
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to bucket: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'AccessDenied', 'Message': f'Permission denied for bucket {bucket_name}'}},
                    'PutObject'
                )
            else:
                logger.error(f"S3 error while writing Parquet file: {e}")
                raise
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Error writing Parquet file: {e}")
            raise
    
    def count_articles(
        self,
        bucket_uri: str
    ) -> int:
        """Count the total number of articles in an S3 bucket/prefix.
        
        This method counts JSON files without loading their content,
        useful for logging total articles at streaming start (Requirement 9.1).
        
        Args:
            bucket_uri: S3 URI in format 's3://bucket-name/prefix/'
            
        Returns:
            int: Total count of JSON article files
            
        Raises:
            ValueError: If bucket_uri format is invalid
            ClientError: If S3 access fails
        """
        bucket_name, prefix = self._parse_s3_uri(bucket_uri)
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pagination_params = {
            'Bucket': bucket_name,
            'Prefix': prefix
        }
        
        count = 0
        for page in paginator.paginate(**pagination_params):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                # Skip directories and non-JSON files
                if not key.endswith('/') and key.endswith('.json'):
                    count += 1
        
        logger.debug(f"Counted {count} articles in s3://{bucket_name}/{prefix}")
        return count
    
    def read_articles_batch(
        self,
        bucket_uri: str,
        batch_size: int = 1000,
        start_after: Optional[str] = None
    ) -> Iterator[List[Dict]]:
        """Read articles from S3 in batches.
        
        Args:
            bucket_uri: S3 URI in format 's3://bucket-name/prefix/'
            batch_size: Number of articles per batch (default: 1000)
            start_after: S3 key to start after (for pagination/resumption)
            
        Yields:
            List[Dict]: Batches of article dictionaries
            
        Raises:
            ValueError: If bucket_uri format is invalid
            ClientError: If S3 access fails
        """
        bucket_name, prefix = self._parse_s3_uri(bucket_uri)
        logger.info(
            f"Starting batch reading from s3://{bucket_name}/{prefix}: "
            f"batch_size={batch_size}, start_after={start_after}"
        )
        
        try:
            # List all article keys first
            all_keys = self._list_article_keys(bucket_name, prefix, start_after)
            
            # Process articles in batches
            batch = []
            total_articles = 0
            batch_num = 0
            
            for key in all_keys:
                try:
                    # Get object from S3
                    response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                    content = response['Body'].read().decode('utf-8')
                    
                    # Parse JSON
                    article = json.loads(content)
                    batch.append(article)
                    total_articles += 1
                    
                    # Yield batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        batch_num += 1
                        logger.info(
                            f"Yielding batch {batch_num}: {len(batch)} articles "
                            f"(total processed: {total_articles})"
                        )
                        yield batch
                        batch = []
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON from {key}: {e}")
                    continue
                except ClientError as e:
                    logger.error(f"Failed to read object {key}: {e}")
                    continue
            
            # Yield remaining articles in final batch
            if batch:
                batch_num += 1
                logger.info(
                    f"Yielding final batch {batch_num}: {len(batch)} articles "
                    f"(total processed: {total_articles})"
                )
                yield batch
            
            logger.info(
                f"Batch reading completed: {total_articles} articles in {batch_num} batches"
            )
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchBucket':
                logger.error(f"Bucket not found: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'NoSuchBucket', 'Message': f'Bucket {bucket_name} does not exist'}},
                    'ListObjectsV2'
                )
            elif error_code == 'AccessDenied':
                logger.error(f"Access denied to bucket: {bucket_name}")
                raise ClientError(
                    {'Error': {'Code': 'AccessDenied', 'Message': f'Permission denied for bucket {bucket_name}'}},
                    'ListObjectsV2'
                )
            else:
                logger.error(f"S3 error while reading articles in batches: {e}")
                raise
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
    
    def _list_article_keys(
        self,
        bucket_name: str,
        prefix: str,
        start_after: Optional[str] = None
    ) -> Iterator[str]:
        """List all article keys from S3 with pagination.
        
        Args:
            bucket_name: S3 bucket name
            prefix: S3 key prefix
            start_after: S3 key to start after (for resumption)
            
        Yields:
            str: S3 object keys for JSON files
            
        Raises:
            ClientError: If S3 access fails
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        # Build pagination parameters
        pagination_params = {
            'Bucket': bucket_name,
            'Prefix': prefix
        }
        if start_after:
            pagination_params['StartAfter'] = start_after
        
        page_iterator = paginator.paginate(**pagination_params)
        
        key_count = 0
        for page in page_iterator:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                
                # Skip directories and non-JSON files
                if key.endswith('/') or not key.endswith('.json'):
                    continue
                
                key_count += 1
                yield key
        
        logger.info(f"Listed {key_count} JSON files from s3://{bucket_name}/{prefix}")
    
    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """Parse S3 URI into bucket name and key/prefix.
        
        Args:
            s3_uri: S3 URI in format 's3://bucket-name/path/to/object'
            
        Returns:
            tuple: (bucket_name, key_or_prefix)
            
        Raises:
            ValueError: If URI format is invalid
        """
        if not s3_uri.startswith('s3://'):
            raise ValueError(f"Invalid S3 URI format: {s3_uri}. Must start with 's3://'")
        
        # Remove 's3://' prefix
        path = s3_uri[5:]
        
        # Split into bucket and key
        parts = path.split('/', 1)
        bucket_name = parts[0]
        key_or_prefix = parts[1] if len(parts) > 1 else ''
        
        if not bucket_name:
            raise ValueError(f"Invalid S3 URI format: {s3_uri}. Bucket name is required")
        
        return bucket_name, key_or_prefix

    async def _download_single_file(
        self,
        s3_client,
        bucket_name: str,
        key: str,
        semaphore: asyncio.Semaphore,
        retry_count: int = 3
    ) -> Optional[Dict]:
        """Download and parse a single S3 file with retry logic and exponential backoff.
        
        Args:
            s3_client: aioboto3 S3 client
            bucket_name: S3 bucket name
            key: S3 object key
            semaphore: Semaphore to limit concurrency
            retry_count: Maximum retry attempts (default: 3)
            
        Returns:
            Parsed article dictionary or None if failed after all retries
        """
        async with semaphore:
            for attempt in range(retry_count):
                try:
                    # Get object from S3
                    response = await s3_client.get_object(Bucket=bucket_name, Key=key)
                    async with response['Body'] as stream:
                        content = await stream.read()
                    
                    # Parse JSON
                    article = json.loads(content.decode('utf-8'))
                    return article
                    
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    
                    # Handle throttling with exponential backoff
                    if error_code in ['SlowDown', 'RequestLimitExceeded', 'ServiceUnavailable']:
                        if attempt < retry_count - 1:
                            # Exponential backoff: 1s, 2s, 4s with jitter
                            backoff_time = (2 ** attempt) * (1.0 + random.uniform(-0.2, 0.2))
                            logger.warning(
                                f"S3 throttling for file_key={key}, retrying in {backoff_time:.2f}s "
                                f"(attempt {attempt + 1}/{retry_count})"
                            )
                            await asyncio.sleep(backoff_time)
                            continue
                        else:
                            logger.error(
                                f"Failed to download file_key={key} after {retry_count} attempts: "
                                f"error_code={error_code}"
                            )
                            return None
                    else:
                        logger.error(f"Failed to download file_key={key}: error_code={error_code}")
                        return None
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON from file_key={key}: error={e}")
                    return None
                    
                except Exception as e:
                    logger.error(f"Unexpected error downloading file_key={key}: error={e}")
                    return None
            
            return None

    async def _download_and_parse_batch(
        self,
        s3_client,
        bucket_name: str,
        keys: List[str],
        semaphore: asyncio.Semaphore
    ) -> tuple[List[Dict], int, int]:
        """Download and parse a batch of S3 objects concurrently.
        
        Args:
            s3_client: aioboto3 S3 client
            bucket_name: S3 bucket name
            keys: List of S3 object keys to download
            semaphore: Semaphore to limit concurrency
            
        Returns:
            Tuple of (articles, failed_downloads, filtered_empty_symbols):
            - articles: List of valid article dictionaries
            - failed_downloads: Count of actual download/parse errors
            - filtered_empty_symbols: Count of articles filtered due to empty symbols
        """
        # Handle empty keys list
        if not keys:
            return [], 0, 0
        
        # Create download tasks for all files in the batch
        tasks = [
            self._download_single_file(s3_client, bucket_name, key, semaphore)
            for key in keys
        ]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Separate failed downloads from articles with empty symbols
        articles = []
        failed_downloads = 0
        filtered_empty_symbols = 0
        
        for article in results:
            if article is None:
                # Actual download/parse error
                failed_downloads += 1
            else:
                # Check if article has symbols
                symbols = article.get('symbols', [])
                if symbols and len(symbols) > 0:
                    articles.append(article)
                else:
                    # Article downloaded successfully but has empty symbols (filtered)
                    filtered_empty_symbols += 1
        
        return articles, failed_downloads, filtered_empty_symbols

    async def read_articles_parallel(
        self,
        bucket_uri: str,
        concurrent_limit: int = 75,
        chunk_size: int = 1000
    ) -> pd.DataFrame:
        """Read all articles from S3 in parallel and return as DataFrame.
        
        Args:
            bucket_uri: S3 URI in format 's3://bucket-name/prefix/'
            concurrent_limit: Maximum concurrent S3 downloads (50-100, default: 75)
            chunk_size: Number of files to process per batch (default: 1000)
            
        Returns:
            DataFrame with columns: id, symbols, content, headline, summary, created_at
            
        Raises:
            ValueError: If bucket_uri format is invalid
            ClientError: If S3 access fails
        """
        bucket_name, prefix = self._parse_s3_uri(bucket_uri)
        logger.info(
            f"Starting parallel S3 reading from s3://{bucket_name}/{prefix}: "
            f"concurrent_limit={concurrent_limit}, chunk_size={chunk_size}"
        )
        
        start_time = time.time()
        
        # List all S3 objects
        all_keys = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    # Skip directories and non-JSON files
                    if not key.endswith('/') and key.endswith('.json'):
                        all_keys.append(key)
        
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            raise
        
        total_files = len(all_keys)
        logger.info(f"Found {total_files} JSON files to download")
        
        if total_files == 0:
            logger.warning("No JSON files found in S3 bucket")
            return pd.DataFrame(columns=['id', 'symbols', 'content', 'headline', 'summary', 'created_at'])
        
        # Process files in chunks
        all_articles = []
        completed = 0
        failed = 0
        filtered = 0
        last_progress_pct = 0
        
        # Create aioboto3 session for async operations
        session = aioboto3.Session()
        
        async with session.client('s3') as s3_client:
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(concurrent_limit)
            
            # Process files in chunks
            for i in range(0, total_files, chunk_size):
                chunk_keys = all_keys[i:i + chunk_size]
                chunk_start = time.time()
                
                # Download and parse batch
                articles, chunk_failed, chunk_filtered = await self._download_and_parse_batch(
                    s3_client, bucket_name, chunk_keys, semaphore
                )
                
                all_articles.extend(articles)
                completed += len(chunk_keys)
                failed += chunk_failed
                filtered += chunk_filtered
                
                # Log progress at 10% intervals
                progress_pct = int((completed / total_files) * 100)
                if progress_pct >= last_progress_pct + 10 or completed == total_files:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Progress: {completed}/{total_files} files ({progress_pct}%), "
                        f"elapsed={elapsed:.1f}s, errors={failed}, filtered={filtered}"
                    )
                    last_progress_pct = progress_pct
                
                chunk_elapsed = time.time() - chunk_start
                logger.info(
                    f"Chunk completed: {len(chunk_keys)} files in {chunk_elapsed:.1f}s, "
                    f"valid={len(articles)}, errors={chunk_failed}, filtered={chunk_filtered}"
                )
        
        # Build DataFrame
        if not all_articles:
            logger.warning("No valid articles found after filtering")
            return pd.DataFrame(columns=['id', 'symbols', 'content', 'headline', 'summary', 'created_at'])
        
        df = pd.DataFrame(all_articles)
        
        # Ensure required columns exist
        required_columns = ['id', 'symbols', 'content', 'headline', 'summary', 'created_at']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[required_columns]
        
        # Calculate memory usage
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        
        total_time = time.time() - start_time
        logger.info(
            f"Parallel S3 reading completed: "
            f"total_time={total_time:.1f}s, "
            f"total_files={total_files}, "
            f"articles_loaded={len(df)}, "
            f"errors={failed}, "
            f"filtered_empty_symbols={filtered}, "
            f"memory_mb={memory_mb:.1f}"
        )
        
        return df
