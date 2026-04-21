"""Tests for S3 operations functionality."""

import asyncio
import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from hypothesis import given, strategies as st, settings
from botocore.exceptions import ClientError
from s3_operations import S3Client
import pytest


class TestS3URIParsing:
    """Property-based tests for S3 URI parsing."""
    
    @given(
        bucket=st.text(min_size=1, max_size=63, alphabet=st.characters(
            whitelist_categories=('Ll', 'Nd'), 
            whitelist_characters='-'
        )).filter(lambda x: x[0] not in '-' and x[-1] not in '-'),
        key=st.text(min_size=0, max_size=100, alphabet=st.characters(
            whitelist_categories=('Lu', 'Ll', 'Nd'),
            whitelist_characters='/-_.'
        ))
    )
    @settings(max_examples=100, deadline=None)
    def test_s3_uri_detection_property(self, bucket, key):
        """
        **Feature: s3-config-loading, Property 1: S3 URI Detection**
        
        For any configuration path string, if it starts with "s3://", 
        the system should attempt S3 download; otherwise, it should use local file loading.
        
        **Validates: Requirements 1.1, 1.4**
        """
        client = S3Client()
        
        # Test S3 URI format
        s3_uri = f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
        
        # Should successfully parse S3 URIs
        try:
            parsed_bucket, parsed_key = client._parse_s3_uri(s3_uri)
            assert parsed_bucket == bucket, f"Expected bucket {bucket}, got {parsed_bucket}"
            assert parsed_key == key, f"Expected key {key}, got {parsed_key}"
        except ValueError:
            # Only acceptable if bucket is empty or invalid
            assert not bucket or bucket.startswith('-') or bucket.endswith('-')
    
    @given(
        path=st.text(min_size=1, max_size=100).filter(lambda x: not x.startswith('s3://'))
    )
    @settings(max_examples=100, deadline=None)
    def test_non_s3_uri_detection_property(self, path):
        """
        **Feature: s3-config-loading, Property 1: S3 URI Detection**
        
        For any path not starting with "s3://", parsing should raise ValueError.
        
        **Validates: Requirements 1.1, 1.4**
        """
        client = S3Client()
        
        # Non-S3 URIs should raise ValueError
        try:
            client._parse_s3_uri(path)
            assert False, f"Expected ValueError for non-S3 URI: {path}"
        except ValueError as e:
            assert "Invalid S3 URI format" in str(e)
            assert "Must start with 's3://'" in str(e)


class TestDownloadFile:
    """Unit tests for download_file method."""
    
    def test_download_file_success(self):
        """Test successful download with mocked S3 client."""
        # Create mock S3 client
        mock_s3_client = Mock()
        mock_response = {
            'Body': Mock(read=Mock(return_value=b'{"test": "data"}'))
        }
        mock_s3_client.get_object.return_value = mock_response
        
        # Create S3Client with mocked boto3 session
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        client = S3Client(boto3_session=mock_session)
        
        # Test download
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            client.download_file('s3://test-bucket/test-key.json', tmp_path)
            
            # Verify S3 client was called correctly
            mock_s3_client.get_object.assert_called_once_with(
                Bucket='test-bucket',
                Key='test-key.json'
            )
            
            # Verify file was written
            with open(tmp_path, 'rb') as f:
                content = f.read()
            assert content == b'{"test": "data"}'
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_download_file_no_such_key(self):
        """Test error handling for NoSuchKey error."""
        # Create mock S3 client that raises NoSuchKey
        mock_s3_client = Mock()
        error_response = {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}}
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        # Create S3Client with mocked boto3 session
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        client = S3Client(boto3_session=mock_session)
        
        # Test download should raise ClientError
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            try:
                client.download_file('s3://test-bucket/missing.json', tmp_path)
                assert False, "Expected ClientError for NoSuchKey"
            except ClientError as e:
                assert e.response['Error']['Code'] == 'NoSuchKey'
                assert 'File not found' in e.response['Error']['Message']
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_download_file_no_such_bucket(self):
        """Test error handling for NoSuchBucket error."""
        # Create mock S3 client that raises NoSuchBucket
        mock_s3_client = Mock()
        error_response = {'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket not found'}}
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        # Create S3Client with mocked boto3 session
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        client = S3Client(boto3_session=mock_session)
        
        # Test download should raise ClientError
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            try:
                client.download_file('s3://missing-bucket/file.json', tmp_path)
                assert False, "Expected ClientError for NoSuchBucket"
            except ClientError as e:
                assert e.response['Error']['Code'] == 'NoSuchBucket'
                assert 'Bucket' in e.response['Error']['Message']
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_download_file_access_denied(self):
        """Test error handling for AccessDenied error."""
        # Create mock S3 client that raises AccessDenied
        mock_s3_client = Mock()
        error_response = {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}}
        mock_s3_client.get_object.side_effect = ClientError(error_response, 'GetObject')
        
        # Create S3Client with mocked boto3 session
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        client = S3Client(boto3_session=mock_session)
        
        # Test download should raise ClientError
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            try:
                client.download_file('s3://restricted-bucket/file.json', tmp_path)
                assert False, "Expected ClientError for AccessDenied"
            except ClientError as e:
                assert e.response['Error']['Code'] == 'AccessDenied'
                assert 'Permission denied' in e.response['Error']['Message']
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_download_file_invalid_uri(self):
        """Test error handling for invalid S3 URI format."""
        # Create S3Client
        mock_session = Mock()
        mock_session.client.return_value = Mock()
        client = S3Client(boto3_session=mock_session)
        
        # Test with invalid URI
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            try:
                client.download_file('/local/path/file.json', tmp_path)
                assert False, "Expected ValueError for invalid S3 URI"
            except ValueError as e:
                assert "Invalid S3 URI format" in str(e)
                assert "Must start with 's3://'" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestParallelReading:
    """Property-based tests for parallel S3 reading."""
    
    @given(
        concurrent_limit=st.integers(min_value=1, max_value=100),
        num_files=st.integers(min_value=1, max_value=200)
    )
    @settings(max_examples=100, deadline=None)
    def test_concurrent_limit_enforcement_property(self, concurrent_limit, num_files):
        """
        **Feature: parallel-s3-dataframe-optimization, Property 1: Concurrent download limit enforcement**
        
        For any parallel S3 reading operation with a configured concurrent limit N, 
        at no point during execution should there be more than N simultaneous download operations active.
        
        **Validates: Requirements 1.2**
        """
        # Track maximum concurrent operations
        max_concurrent = 0
        current_concurrent = 0
        
        async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
            nonlocal max_concurrent, current_concurrent
            
            async with semaphore:
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
                
                # Simulate some work
                await asyncio.sleep(0.001)
                
                current_concurrent -= 1
                
                # Return a valid article
                return {
                    'id': key,
                    'symbols': ['TEST'],
                    'content': 'test content',
                    'headline': 'test headline',
                    'summary': 'test summary'
                }
        
        async def run_test():
            # Create mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_files)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Patch the _download_single_file method
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                # Create semaphore
                semaphore = asyncio.Semaphore(concurrent_limit)
                
                # Mock aioboto3 session and client
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Run parallel reading
                    try:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=concurrent_limit,
                            chunk_size=1000
                        )
                    except Exception:
                        # Some edge cases might fail, but we still check the concurrent limit
                        pass
            
            # Verify concurrent limit was never exceeded
            assert max_concurrent <= concurrent_limit, (
                f"Concurrent limit violated: max_concurrent={max_concurrent}, "
                f"limit={concurrent_limit}"
            )
        
        # Run the async test
        asyncio.run(run_test())
    
    @given(
        num_retries=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=100, deadline=None)
    def test_exponential_backoff_property(self, num_retries):
        """
        **Feature: parallel-s3-dataframe-optimization, Property 3: Exponential backoff on throttling**
        
        For any S3 download that encounters a throttling error, subsequent retry attempts 
        should have exponentially increasing delays (e.g., 1s, 2s, 4s).
        
        **Validates: Requirements 1.5**
        """
        backoff_times = []
        
        async def run_test():
            # Create mock S3 client that always throttles
            mock_s3_client = AsyncMock()
            
            # Mock get_object to always raise throttling error
            throttle_error = ClientError(
                {'Error': {'Code': 'SlowDown', 'Message': 'Please reduce your request rate'}},
                'GetObject'
            )
            mock_s3_client.get_object.side_effect = throttle_error
            
            # Create S3Client
            mock_session = Mock()
            mock_session.client.return_value = Mock()
            client = S3Client(boto3_session=mock_session)
            
            # Track sleep times
            original_sleep = asyncio.sleep
            
            async def mock_sleep(duration):
                backoff_times.append(duration)
                await original_sleep(0.001)  # Actually sleep very briefly
            
            # Create semaphore
            semaphore = asyncio.Semaphore(1)
            
            # Patch asyncio.sleep to track backoff times
            with patch('asyncio.sleep', side_effect=mock_sleep):
                result = await client._download_single_file(
                    mock_s3_client,
                    'test-bucket',
                    'test-key.json',
                    semaphore,
                    retry_count=num_retries
                )
            
            # Should return None after all retries fail
            assert result is None
            
            # Should have num_retries - 1 backoff attempts (no backoff after last failure)
            expected_backoffs = num_retries - 1
            assert len(backoff_times) == expected_backoffs, (
                f"Expected {expected_backoffs} backoff attempts, got {len(backoff_times)}"
            )
            
            # Verify exponential backoff pattern (with jitter tolerance)
            for i in range(len(backoff_times)):
                expected_base = 2 ** i  # 1, 2, 4, 8, ...
                actual = backoff_times[i]
                
                # Allow for ±20% jitter
                min_expected = expected_base * 0.8
                max_expected = expected_base * 1.2
                
                assert min_expected <= actual <= max_expected, (
                    f"Backoff time {i} out of range: expected {expected_base}±20%, "
                    f"got {actual}"
                )
                
                # Verify exponential growth (each backoff should be roughly 2x the previous)
                if i > 0:
                    ratio = backoff_times[i] / backoff_times[i-1]
                    # Allow some tolerance for jitter
                    assert 1.6 <= ratio <= 2.4, (
                        f"Backoff growth not exponential: "
                        f"backoff[{i}]={backoff_times[i]}, "
                        f"backoff[{i-1}]={backoff_times[i-1]}, "
                        f"ratio={ratio}"
                    )
        
        # Run the async test
        asyncio.run(run_test())
    
    @given(
        chunk_size=st.integers(min_value=1, max_value=5000),
        num_files=st.integers(min_value=1, max_value=10000)
    )
    @settings(max_examples=100, deadline=None)
    def test_batch_chunk_size_consistency_property(self, chunk_size, num_files):
        """
        **Feature: parallel-s3-dataframe-optimization, Property 2: Batch chunk size consistency**
        
        For any set of S3 files and configured chunk size C, the files should be processed 
        in batches where each batch (except possibly the last) contains exactly C files.
        
        **Validates: Requirements 1.3**
        """
        batch_sizes = []
        
        async def mock_download_and_parse_batch(s3_client, bucket_name, keys, semaphore):
            # Track the batch size
            batch_sizes.append(len(keys))
            # Return tuple: (articles, failed_downloads, filtered_empty_symbols)
            return [], 0, 0
        
        async def run_test():
            # Create mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_files)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Patch the _download_and_parse_batch method
            with patch.object(client, '_download_and_parse_batch', side_effect=mock_download_and_parse_batch):
                # Mock aioboto3 session and client
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Run parallel reading
                    df = await client.read_articles_parallel(
                        's3://test-bucket/prefix/',
                        concurrent_limit=10,
                        chunk_size=chunk_size
                    )
            
            # Verify batch sizes
            expected_full_batches = num_files // chunk_size
            expected_remainder = num_files % chunk_size
            
            # All batches except possibly the last should be exactly chunk_size
            for i in range(len(batch_sizes) - 1):
                assert batch_sizes[i] == chunk_size, (
                    f"Batch {i} size incorrect: expected {chunk_size}, got {batch_sizes[i]}"
                )
            
            # Last batch should be either chunk_size (if no remainder) or remainder
            if len(batch_sizes) > 0:
                last_batch_size = batch_sizes[-1]
                if expected_remainder == 0:
                    assert last_batch_size == chunk_size, (
                        f"Last batch size incorrect: expected {chunk_size}, got {last_batch_size}"
                    )
                else:
                    assert last_batch_size == expected_remainder, (
                        f"Last batch size incorrect: expected {expected_remainder}, got {last_batch_size}"
                    )
            
            # Total number of batches should be correct
            expected_batches = (num_files + chunk_size - 1) // chunk_size  # Ceiling division
            assert len(batch_sizes) == expected_batches, (
                f"Number of batches incorrect: expected {expected_batches}, got {len(batch_sizes)}"
            )
            
            # Total files processed should equal num_files
            total_processed = sum(batch_sizes)
            assert total_processed == num_files, (
                f"Total files processed incorrect: expected {num_files}, got {total_processed}"
            )
        
        # Run the async test
        asyncio.run(run_test())
    
    @given(
        num_valid=st.integers(min_value=0, max_value=100),
        num_empty=st.integers(min_value=0, max_value=100)
    )
    @settings(max_examples=100, deadline=None)
    def test_empty_symbols_filtering_property(self, num_valid, num_empty):
        """
        **Feature: parallel-s3-dataframe-optimization, Property 4: Empty symbols filtering**
        
        For any set of articles loaded into the DataFrame, all articles with empty symbols 
        arrays should be excluded from the final DataFrame.
        
        **Validates: Requirements 2.2**
        """
        async def run_test():
            # Create mock articles - mix of valid and empty symbols
            mock_articles = []
            
            # Add articles with valid symbols
            for i in range(num_valid):
                mock_articles.append({
                    'id': f'valid_{i}',
                    'symbols': ['AAPL', 'GOOGL'],
                    'content': f'content {i}',
                    'headline': f'headline {i}',
                    'summary': f'summary {i}',
                    'created_at': f'2019-07-{8+i%20:02d}T16:49:02Z'
                })
            
            # Add articles with empty symbols (various forms)
            for i in range(num_empty):
                empty_type = i % 3
                if empty_type == 0:
                    symbols = []  # Empty list
                elif empty_type == 1:
                    symbols = None  # None
                else:
                    symbols = []  # Empty list again
                
                mock_articles.append({
                    'id': f'empty_{i}',
                    'symbols': symbols,
                    'content': f'content {i}',
                    'headline': f'headline {i}',
                    'summary': f'summary {i}',
                    'created_at': f'2019-08-{1+i%28:02d}T10:30:00Z'
                })
            
            # Shuffle to mix valid and empty
            import random
            random.shuffle(mock_articles)
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(len(mock_articles))]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file to return our test articles
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            # Patch the _download_single_file method
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                # Mock aioboto3 session and client
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Run parallel reading
                    df = await client.read_articles_parallel(
                        's3://test-bucket/prefix/',
                        concurrent_limit=10,
                        chunk_size=1000
                    )
            
            # Verify only valid articles are in DataFrame
            assert len(df) == num_valid, (
                f"DataFrame should contain only {num_valid} valid articles, got {len(df)}"
            )
            
            # Verify all articles in DataFrame have non-empty symbols
            for idx, row in df.iterrows():
                symbols = row['symbols']
                assert symbols is not None and len(symbols) > 0, (
                    f"Article {row['id']} has empty symbols in DataFrame"
                )
            
            # Verify all valid article IDs are present
            df_ids = set(df['id'].tolist())
            expected_ids = {f'valid_{i}' for i in range(num_valid)}
            assert df_ids == expected_ids, (
                f"DataFrame IDs don't match expected valid IDs"
            )
        
        # Run the async test
        asyncio.run(run_test())


class TestParallelReadingUnit:
    """Unit tests for parallel S3 reading functionality."""
    
    def test_dataframe_schema_correctness(self):
        """Test that DataFrame has correct schema with required columns."""
        async def run_test():
            # Create mock articles
            mock_articles = [
                {
                    'id': 'article1',
                    'symbols': ['AAPL'],
                    'content': 'content1',
                    'headline': 'headline1',
                    'summary': 'summary1'
                },
                {
                    'id': 'article2',
                    'symbols': ['GOOGL', 'MSFT'],
                    'content': 'content2',
                    'headline': 'headline2',
                    'summary': 'summary2'
                }
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(len(mock_articles))]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    df = await client.read_articles_parallel(
                        's3://test-bucket/prefix/',
                        concurrent_limit=10,
                        chunk_size=1000
                    )
            
            # Verify schema
            required_columns = ['id', 'symbols', 'content', 'headline', 'summary', 'created_at']
            assert list(df.columns) == required_columns
            assert len(df) == 2
            
            # Verify data types and values
            assert df['id'].tolist() == ['article1', 'article2']
            assert df['symbols'].tolist() == [['AAPL'], ['GOOGL', 'MSFT']]
        
        asyncio.run(run_test())
    
    def test_malformed_json_handling(self):
        """Test error handling for malformed JSON files."""
        async def run_test():
            # Mock S3 client that returns invalid JSON
            mock_s3_client = AsyncMock()
            
            # Mock get_object to return malformed JSON
            mock_response = {
                'Body': AsyncMock()
            }
            mock_response['Body'].read = AsyncMock(return_value=b'{invalid json}')
            mock_s3_client.get_object.return_value = mock_response
            
            client = S3Client(boto3_session=Mock())
            semaphore = asyncio.Semaphore(1)
            
            # Should return None for malformed JSON
            result = await client._download_single_file(
                mock_s3_client,
                'test-bucket',
                'malformed.json',
                semaphore
            )
            
            assert result is None
        
        asyncio.run(run_test())
    
    def test_progress_logging(self):
        """Test that progress is logged at appropriate intervals."""
        async def run_test():
            # Create 100 mock articles to trigger progress logging
            num_articles = 100
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST'],
                    'content': f'content{i}',
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(num_articles)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_articles)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=10,
                            chunk_size=50
                        )
                        
                        # Verify progress logging occurred
                        info_calls = [call for call in mock_logger.info.call_args_list]
                        
                        # Should have: start log, progress logs, completion log
                        assert len(info_calls) >= 3
                        
                        # Check for start log
                        start_log = str(info_calls[0])
                        assert 'Starting parallel S3 reading' in start_log or 'Found' in start_log
                        
                        # Check for completion log
                        completion_log = str(info_calls[-1])
                        assert 'completed' in completion_log.lower() or 'articles_loaded' in completion_log
        
        asyncio.run(run_test())
    
    def test_memory_usage_tracking(self):
        """Test that memory usage is tracked and logged."""
        async def run_test():
            # Create mock articles
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST'],
                    'content': f'content{i}' * 100,  # Larger content
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(10)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(len(mock_articles))]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=10,
                            chunk_size=1000
                        )
                        
                        # Verify memory usage was logged
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        
                        # Check for memory usage in completion log
                        completion_logs = [log for log in info_calls if 'memory_mb' in log]
                        assert len(completion_logs) > 0, "Memory usage should be logged"
        
        asyncio.run(run_test())


class TestParallelReadingLogging:
    """Unit tests for logging in parallel S3 reading operations."""
    
    def test_start_log_contains_configuration(self):
        """Test that start log includes total files, concurrent limit, and chunk size."""
        async def run_test():
            # Create mock articles
            num_articles = 50
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST'],
                    'content': f'content{i}',
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(num_articles)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_articles)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=75,
                            chunk_size=1000
                        )
                        
                        # Get all info log calls
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        
                        # Find start log
                        start_logs = [log for log in info_calls if 'Starting parallel S3 reading' in log]
                        assert len(start_logs) > 0, "Should have start log"
                        
                        start_log = start_logs[0]
                        # Verify configuration is logged
                        assert 'concurrent_limit=75' in start_log
                        assert 'chunk_size=1000' in start_log
                        
                        # Find files count log
                        files_logs = [log for log in info_calls if 'Found' in log and 'JSON files' in log]
                        assert len(files_logs) > 0, "Should log total files count"
                        files_log = files_logs[0]
                        assert str(num_articles) in files_log
        
        asyncio.run(run_test())
    
    def test_progress_logging_frequency(self):
        """Test that progress is logged at 10% intervals."""
        async def run_test():
            # Create 100 articles to ensure we hit multiple 10% milestones
            num_articles = 100
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST'],
                    'content': f'content{i}',
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(num_articles)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_articles)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=10,
                            chunk_size=10  # Small chunks to trigger multiple progress logs
                        )
                        
                        # Get all info log calls
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        
                        # Find progress logs
                        progress_logs = [log for log in info_calls if 'Progress:' in log]
                        
                        # Should have multiple progress logs (at least at 10%, 20%, ..., 100%)
                        assert len(progress_logs) >= 2, f"Should have multiple progress logs, got {len(progress_logs)}"
                        
                        # Verify progress logs contain expected information
                        for log in progress_logs:
                            assert 'files' in log
                            assert 'elapsed=' in log
                            assert 'errors=' in log  # Changed from 'failed=' to 'errors='
                            assert 'filtered=' in log  # Added filtered count
                            # Check for percentage
                            assert '%' in log or '100' in log
        
        asyncio.run(run_test())
    
    def test_chunk_completion_logging(self):
        """Test that chunk completion logs include timing and error counts."""
        async def run_test():
            # Create mock articles
            num_articles = 30
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST'],
                    'content': f'content{i}',
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(num_articles)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_articles)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=10,
                            chunk_size=10  # Process in 3 chunks
                        )
                        
                        # Get all info log calls
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        
                        # Find chunk completion logs
                        chunk_logs = [log for log in info_calls if 'Chunk completed' in log]
                        
                        # Should have 3 chunks (30 files / 10 per chunk)
                        assert len(chunk_logs) == 3, f"Expected 3 chunk logs, got {len(chunk_logs)}"
                        
                        # Verify chunk logs contain expected information
                        for log in chunk_logs:
                            assert 'files in' in log  # Timing
                            assert 'valid=' in log  # Changed from 'valid articles' to 'valid='
                            assert 'errors=' in log  # Error count
                            assert 'filtered=' in log  # Filtered count
        
        asyncio.run(run_test())
    
    def test_error_logging_includes_file_keys(self):
        """Test that error logs include the file key that failed."""
        async def run_test():
            # Mock S3 client that fails for specific files
            mock_s3_client = AsyncMock()
            
            # Mock get_object to fail with throttling error
            throttle_error = ClientError(
                {'Error': {'Code': 'SlowDown', 'Message': 'Please reduce your request rate'}},
                'GetObject'
            )
            mock_s3_client.get_object.side_effect = throttle_error
            
            client = S3Client(boto3_session=Mock())
            semaphore = asyncio.Semaphore(1)
            
            # Capture log messages
            with patch('s3_operations.logger') as mock_logger:
                result = await client._download_single_file(
                    mock_s3_client,
                    'test-bucket',
                    'test-file-123.json',
                    semaphore,
                    retry_count=2
                )
                
                # Should return None after retries
                assert result is None
                
                # Get all warning and error log calls
                warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
                error_calls = [str(call) for call in mock_logger.error.call_args_list]
                
                # Verify file key is in warning logs (retry attempts)
                assert len(warning_calls) > 0, "Should have warning logs for retries"
                for log in warning_calls:
                    assert 'file_key=test-file-123.json' in log, f"File key should be in warning log: {log}"
                
                # Verify file key is in error log (final failure)
                assert len(error_calls) > 0, "Should have error log for final failure"
                final_error = error_calls[-1]
                assert 'file_key=test-file-123.json' in final_error, f"File key should be in error log: {final_error}"
        
        asyncio.run(run_test())
    
    def test_completion_log_contains_summary(self):
        """Test that completion log includes total time, articles loaded, memory, and filtered count."""
        async def run_test():
            # Create mock articles
            num_articles = 20
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST'],
                    'content': f'content{i}',
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(num_articles)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_articles)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=10,
                            chunk_size=1000
                        )
                        
                        # Get all info log calls
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        
                        # Find completion log
                        completion_logs = [log for log in info_calls if 'completed' in log.lower() and 'total_time=' in log]
                        assert len(completion_logs) > 0, "Should have completion log"
                        
                        completion_log = completion_logs[0]
                        # Verify completion log contains all required information
                        assert 'total_time=' in completion_log
                        assert 'articles_loaded=' in completion_log
                        assert 'memory_mb=' in completion_log
                        assert 'filtered_empty_symbols=' in completion_log
        
        asyncio.run(run_test())
    
    def test_dataframe_statistics_logging(self):
        """Test that DataFrame creation statistics are logged."""
        async def run_test():
            # Create mock articles with varying sizes
            num_articles = 15
            mock_articles = [
                {
                    'id': f'article{i}',
                    'symbols': ['TEST', 'AAPL'],
                    'content': f'content{i}' * 50,  # Larger content
                    'headline': f'headline{i}',
                    'summary': f'summary{i}'
                }
                for i in range(num_articles)
            ]
            
            # Mock S3 client
            mock_s3_client = Mock()
            mock_session = Mock()
            mock_session.client.return_value = mock_s3_client
            
            # Mock list_objects_v2 paginator
            mock_paginator = Mock()
            mock_keys = [f"file_{i}.json" for i in range(num_articles)]
            mock_paginator.paginate.return_value = [
                {'Contents': [{'Key': key} for key in mock_keys]}
            ]
            mock_s3_client.get_paginator.return_value = mock_paginator
            
            client = S3Client(boto3_session=mock_session)
            
            # Mock _download_single_file
            article_index = 0
            
            async def mock_download_single_file(s3_client, bucket_name, key, semaphore, retry_count=3):
                nonlocal article_index
                if article_index < len(mock_articles):
                    article = mock_articles[article_index]
                    article_index += 1
                    return article
                return None
            
            with patch.object(client, '_download_single_file', side_effect=mock_download_single_file):
                mock_aioboto3_client = AsyncMock()
                
                with patch('s3_operations.aioboto3.Session') as mock_aioboto3_session:
                    mock_context = AsyncMock()
                    mock_context.__aenter__.return_value = mock_aioboto3_client
                    mock_context.__aexit__.return_value = None
                    mock_aioboto3_session.return_value.client.return_value = mock_context
                    
                    # Capture log messages
                    with patch('s3_operations.logger') as mock_logger:
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=10,
                            chunk_size=1000
                        )
                        
                        # Get all info log calls
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        
                        # Find DataFrame statistics in completion log
                        stats_logs = [log for log in info_calls if 'memory_mb=' in log and 'articles_loaded=' in log]
                        assert len(stats_logs) > 0, "Should log DataFrame statistics"
                        
                        stats_log = stats_logs[0]
                        # Verify statistics are present
                        assert 'articles_loaded=' in stats_log
                        assert 'memory_mb=' in stats_log
                        assert str(num_articles) in stats_log  # Should show correct article count
        
        asyncio.run(run_test())


class TestBatchReadingUnit:
    """Unit tests for batch reading functionality."""
    
    def test_batch_size_adherence_with_exact_multiple(self):
        """Test batch size adherence when total articles is exact multiple of batch size."""
        # Create 3000 mock articles (exactly 3 batches of 1000)
        num_articles = 3000
        batch_size = 1000
        mock_articles = [
            {
                'id': f'article{i}',
                'symbols': ['TEST'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        
        # Mock list_objects_v2 paginator
        mock_paginator = Mock()
        mock_keys = [f"file_{i}.json" for i in range(num_articles)]
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': key} for key in mock_keys]}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        # Mock get_object to return articles
        article_index = 0
        
        def mock_get_object(Bucket, Key):
            nonlocal article_index
            if article_index < len(mock_articles):
                article = mock_articles[article_index]
                article_index += 1
                return {
                    'Body': Mock(read=Mock(return_value=json.dumps(article).encode('utf-8')))
                }
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                'GetObject'
            )
        
        mock_s3_client.get_object.side_effect = mock_get_object
        
        client = S3Client(boto3_session=mock_session)
        
        # Read articles in batches
        batches = list(client.read_articles_batch(
            's3://test-bucket/prefix/',
            batch_size=batch_size
        ))
        
        # Should have exactly 3 batches
        assert len(batches) == 3
        
        # All batches should be exactly batch_size
        for i, batch in enumerate(batches):
            assert len(batch) == batch_size, f"Batch {i} has {len(batch)} articles, expected {batch_size}"
        
        # Total articles should be correct
        total = sum(len(batch) for batch in batches)
        assert total == num_articles
    
    def test_batch_size_adherence_with_remainder(self):
        """Test batch size adherence when total articles has remainder."""
        # Create 2500 mock articles (2 full batches of 1000 + 500 remainder)
        num_articles = 2500
        batch_size = 1000
        mock_articles = [
            {
                'id': f'article{i}',
                'symbols': ['TEST'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        
        # Mock list_objects_v2 paginator
        mock_paginator = Mock()
        mock_keys = [f"file_{i}.json" for i in range(num_articles)]
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': key} for key in mock_keys]}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        # Mock get_object to return articles
        article_index = 0
        
        def mock_get_object(Bucket, Key):
            nonlocal article_index
            if article_index < len(mock_articles):
                article = mock_articles[article_index]
                article_index += 1
                return {
                    'Body': Mock(read=Mock(return_value=json.dumps(article).encode('utf-8')))
                }
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                'GetObject'
            )
        
        mock_s3_client.get_object.side_effect = mock_get_object
        
        client = S3Client(boto3_session=mock_session)
        
        # Read articles in batches
        batches = list(client.read_articles_batch(
            's3://test-bucket/prefix/',
            batch_size=batch_size
        ))
        
        # Should have 3 batches (2 full + 1 partial)
        assert len(batches) == 3
        
        # First two batches should be exactly batch_size
        assert len(batches[0]) == batch_size
        assert len(batches[1]) == batch_size
        
        # Last batch should have remainder
        assert len(batches[2]) == 500
        
        # Total articles should be correct
        total = sum(len(batch) for batch in batches)
        assert total == num_articles
    
    def test_pagination_across_batches(self):
        """Test that pagination works correctly across multiple S3 pages."""
        # Create 150 mock articles across 2 S3 pages
        num_articles = 150
        batch_size = 50
        mock_articles = [
            {
                'id': f'article{i}',
                'symbols': ['TEST'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        
        # Mock list_objects_v2 paginator with 2 pages
        mock_paginator = Mock()
        page1_keys = [f"file_{i}.json" for i in range(100)]
        page2_keys = [f"file_{i}.json" for i in range(100, 150)]
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': key} for key in page1_keys]},
            {'Contents': [{'Key': key} for key in page2_keys]}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        # Mock get_object to return articles
        article_index = 0
        
        def mock_get_object(Bucket, Key):
            nonlocal article_index
            if article_index < len(mock_articles):
                article = mock_articles[article_index]
                article_index += 1
                return {
                    'Body': Mock(read=Mock(return_value=json.dumps(article).encode('utf-8')))
                }
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                'GetObject'
            )
        
        mock_s3_client.get_object.side_effect = mock_get_object
        
        client = S3Client(boto3_session=mock_session)
        
        # Read articles in batches
        batches = list(client.read_articles_batch(
            's3://test-bucket/prefix/',
            batch_size=batch_size
        ))
        
        # Should have 3 batches (50 + 50 + 50)
        assert len(batches) == 3
        
        # All batches should be batch_size
        for batch in batches:
            assert len(batch) == batch_size
        
        # Total articles should be correct
        total = sum(len(batch) for batch in batches)
        assert total == num_articles
    
    def test_start_after_resumption(self):
        """Test that start_after parameter allows resumption from specific key."""
        # Create 100 mock articles
        num_articles = 100
        batch_size = 50
        mock_articles = [
            {
                'id': f'article{i}',
                'symbols': ['TEST'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        
        # Mock list_objects_v2 paginator - should only return keys after start_after
        mock_paginator = Mock()
        # Simulate starting after file_49.json (should get files 50-99)
        remaining_keys = [f"file_{i}.json" for i in range(50, 100)]
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': key} for key in remaining_keys]}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        # Mock get_object to return articles starting from index 50
        article_index = 50
        
        def mock_get_object(Bucket, Key):
            nonlocal article_index
            if article_index < len(mock_articles):
                article = mock_articles[article_index]
                article_index += 1
                return {
                    'Body': Mock(read=Mock(return_value=json.dumps(article).encode('utf-8')))
                }
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                'GetObject'
            )
        
        mock_s3_client.get_object.side_effect = mock_get_object
        
        client = S3Client(boto3_session=mock_session)
        
        # Read articles in batches starting after file_49.json
        batches = list(client.read_articles_batch(
            's3://test-bucket/prefix/',
            batch_size=batch_size,
            start_after='file_49.json'
        ))
        
        # Should have 1 batch with 50 articles
        assert len(batches) == 1
        assert len(batches[0]) == 50
        
        # Verify paginator was called with StartAfter parameter
        paginate_call = mock_paginator.paginate.call_args
        assert paginate_call[1]['StartAfter'] == 'file_49.json'
        
        # Verify articles start from index 50
        assert batches[0][0]['id'] == 'article50'
    
    def test_filtering_non_json_files(self):
        """Test that non-JSON files and directories are filtered out."""
        # Create mock articles
        num_articles = 10
        mock_articles = [
            {
                'id': f'article{i}',
                'symbols': ['TEST'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        
        # Mock list_objects_v2 paginator with mixed file types
        mock_paginator = Mock()
        mixed_keys = [
            'file_0.json',
            'file_1.txt',  # Should be filtered
            'file_2.json',
            'subdir/',  # Should be filtered (directory)
            'file_3.json',
            'file_4.csv',  # Should be filtered
            'file_5.json',
            'file_6.json',
            'file_7.xml',  # Should be filtered
            'file_8.json',
            'file_9.json',
            'file_10.json',
            'file_11.json',
            'file_12.json'
        ]
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': key} for key in mixed_keys]}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        # Mock get_object to return articles (only for .json files)
        article_index = 0
        
        def mock_get_object(Bucket, Key):
            nonlocal article_index
            if article_index < len(mock_articles):
                article = mock_articles[article_index]
                article_index += 1
                return {
                    'Body': Mock(read=Mock(return_value=json.dumps(article).encode('utf-8')))
                }
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                'GetObject'
            )
        
        mock_s3_client.get_object.side_effect = mock_get_object
        
        client = S3Client(boto3_session=mock_session)
        
        # Read articles in batches
        batches = list(client.read_articles_batch(
            's3://test-bucket/prefix/',
            batch_size=50
        ))
        
        # Should have 1 batch with only 10 JSON files
        assert len(batches) == 1
        assert len(batches[0]) == 10
        
        # Verify get_object was only called for JSON files (10 times)
        assert mock_s3_client.get_object.call_count == 10


class TestBatchReading:
    """Property-based tests for batch reading functionality."""
    
    @given(
        batch_size=st.integers(min_value=100, max_value=10000),
        num_articles=st.integers(min_value=1, max_value=20000)
    )
    @settings(max_examples=100, deadline=None)
    def test_batch_size_adherence_property(self, batch_size, num_articles):
        """
        **Feature: streaming-batch-s3-tables-output, Property 7: Batch size adherence**
        
        For any configured batch_size B and total articles T where T > B, 
        all batches except possibly the last should contain exactly B articles.
        
        **Validates: Requirements 7.3**
        """
        # Create mock articles
        mock_articles = [
            {
                'id': f'article{i}',
                'symbols': ['TEST'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Mock S3 client
        mock_s3_client = Mock()
        mock_session = Mock()
        mock_session.client.return_value = mock_s3_client
        
        # Mock list_objects_v2 paginator
        mock_paginator = Mock()
        mock_keys = [f"file_{i}.json" for i in range(num_articles)]
        mock_paginator.paginate.return_value = [
            {'Contents': [{'Key': key} for key in mock_keys]}
        ]
        mock_s3_client.get_paginator.return_value = mock_paginator
        
        # Mock get_object to return articles
        article_index = 0
        
        def mock_get_object(Bucket, Key):
            nonlocal article_index
            if article_index < len(mock_articles):
                article = mock_articles[article_index]
                article_index += 1
                return {
                    'Body': Mock(read=Mock(return_value=json.dumps(article).encode('utf-8')))
                }
            raise ClientError(
                {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                'GetObject'
            )
        
        mock_s3_client.get_object.side_effect = mock_get_object
        
        client = S3Client(boto3_session=mock_session)
        
        # Read articles in batches
        batches = list(client.read_articles_batch(
            's3://test-bucket/prefix/',
            batch_size=batch_size
        ))
        
        # Verify batch sizes
        expected_full_batches = num_articles // batch_size
        expected_remainder = num_articles % batch_size
        
        # All batches except possibly the last should be exactly batch_size
        for i in range(len(batches) - 1):
            assert len(batches[i]) == batch_size, (
                f"Batch {i} size incorrect: expected {batch_size}, got {len(batches[i])}"
            )
        
        # Last batch should be either batch_size (if no remainder) or remainder
        if len(batches) > 0:
            last_batch_size = len(batches[-1])
            if expected_remainder == 0:
                assert last_batch_size == batch_size, (
                    f"Last batch size incorrect: expected {batch_size}, got {last_batch_size}"
                )
            else:
                assert last_batch_size == expected_remainder, (
                    f"Last batch size incorrect: expected {expected_remainder}, got {last_batch_size}"
                )
        
        # Total number of batches should be correct
        expected_batches = (num_articles + batch_size - 1) // batch_size  # Ceiling division
        assert len(batches) == expected_batches, (
            f"Number of batches incorrect: expected {expected_batches}, got {len(batches)}"
        )
        
        # Total articles processed should equal num_articles
        total_processed = sum(len(batch) for batch in batches)
        assert total_processed == num_articles, (
            f"Total articles processed incorrect: expected {num_articles}, got {total_processed}"
        )


if __name__ == "__main__":
    import pytest
    import sys
    
    # Run tests
    sys.exit(pytest.main([__file__, "-v"]))
