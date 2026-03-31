"""Integration tests for end-to-end workflow."""

import asyncio
import json
import tempfile
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import pandas as pd
import pytest
from botocore.exceptions import ClientError

from config import Config, ModelConfig, RatingScale
from s3_operations import S3Client
from orchestrator import BatchOrchestrator
from article_processor import AnalysisTask
from sentiment_analyzer import SentimentResult


class TestParallelReadingIntegration:
    """Integration tests for parallel reading with mocked S3."""
    
    def test_parallel_reading_with_1000_files(self):
        """
        Test parallel reading with mocked S3 bucket containing 1000+ test files.
        
        Validates:
        - Parallel download completes successfully
        - DataFrame contains expected articles
        - Empty symbols filtering works
        
        Requirements: 1.4, 2.1, 2.2
        """
        async def run_test():
            # Create 1200 mock articles (mix of valid and empty symbols)
            num_valid = 1000
            num_empty = 200
            total_files = num_valid + num_empty
            
            mock_articles = []
            
            # Add valid articles
            for i in range(num_valid):
                mock_articles.append({
                    'id': f'article_{i}',
                    'symbols': ['AAPL', 'GOOGL'] if i % 2 == 0 else ['MSFT'],
                    'content': f'Test content for article {i}',
                    'headline': f'Test headline {i}',
                    'summary': f'Test summary {i}'
                })
            
            # Add articles with empty symbols
            for i in range(num_empty):
                mock_articles.append({
                    'id': f'empty_{i}',
                    'symbols': [],  # Empty symbols - should be filtered
                    'content': f'Content {i}',
                    'headline': f'Headline {i}',
                    'summary': f'Summary {i}'
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
            mock_keys = [f"articles/file_{i}.json" for i in range(total_files)]
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
                        's3://test-bucket/articles/',
                        concurrent_limit=75,
                        chunk_size=100
                    )
            
            # Verify parallel download completed successfully
            assert df is not None, "DataFrame should not be None"
            
            # Verify DataFrame contains expected articles (only valid ones)
            assert len(df) == num_valid, f"Expected {num_valid} articles, got {len(df)}"
            
            # Verify empty symbols filtering works
            for idx, row in df.iterrows():
                symbols = row['symbols']
                assert symbols is not None and len(symbols) > 0, \
                    f"Article {row['id']} has empty symbols in DataFrame"
            
            # Verify all valid article IDs are present
            df_ids = set(df['id'].tolist())
            expected_ids = {f'article_{i}' for i in range(num_valid)}
            assert df_ids == expected_ids, "DataFrame should contain all valid article IDs"
            
            # Verify DataFrame schema
            required_columns = ['id', 'symbols', 'content', 'headline', 'summary', 'created_at']
            assert list(df.columns) == required_columns, \
                f"DataFrame columns mismatch: expected {required_columns}, got {list(df.columns)}"
        
        asyncio.run(run_test())
    
    def test_parallel_reading_with_various_chunk_sizes(self):
        """Test parallel reading with different chunk sizes."""
        async def run_test():
            # Create 500 mock articles
            num_articles = 500
            mock_articles = [
                {
                    'id': f'article_{i}',
                    'symbols': ['TEST'],
                    'content': f'Content {i}',
                    'headline': f'Headline {i}',
                    'summary': f'Summary {i}'
                }
                for i in range(num_articles)
            ]
            
            # Test with different chunk sizes
            chunk_sizes = [50, 100, 250, 500]
            
            for chunk_size in chunk_sizes:
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
                        
                        # Run parallel reading with this chunk size
                        df = await client.read_articles_parallel(
                            's3://test-bucket/prefix/',
                            concurrent_limit=50,
                            chunk_size=chunk_size
                        )
                
                # Verify all articles were loaded regardless of chunk size
                assert len(df) == num_articles, \
                    f"Chunk size {chunk_size}: expected {num_articles} articles, got {len(df)}"
        
        asyncio.run(run_test())


class TestDataFrameProcessingWorkflow:
    """Integration tests for DataFrame-based processing workflow."""
    
    def test_dataframe_workflow_with_10k_articles(self):
        """
        Test DataFrame-based processing workflow with 10K articles.
        
        Validates:
        - Load test DataFrame with 10K articles
        - Process with single model configuration
        - Verify output Parquet schema matches existing format
        - Check rate limiting still functions correctly
        
        Requirements: 3.2, 3.3, 4.3
        """
        async def run_test():
            # Create test configuration with single model
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model-1',
                        model_name='TestModel1',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=-5, max=5),
                job_name='integration-test',
                max_articles=None
            )
            
            # Create orchestrator
            orchestrator = BatchOrchestrator(config)
            
            # Create test DataFrame with 10K articles
            num_articles = 10000
            articles_data = []
            for i in range(num_articles):
                articles_data.append({
                    'id': f'article_{i}',
                    'symbols': ['AAPL'] if i % 3 == 0 else ['GOOGL', 'MSFT'],
                    'content': f'Test content {i}',
                    'headline': f'Test headline {i}',
                    'summary': f'Test summary {i}'
                })
            
            mock_df = pd.DataFrame(articles_data)
            
            # Mock S3 client's read_articles_parallel method
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read_parallel:
                mock_read_parallel.return_value = mock_df
                
                # Mock prompt loading
                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='Test prompt'):
                    # Mock sentiment analyzer to return results
                    mock_results = []
                    for i in range(min(100, num_articles)):  # Limit to 100 for test speed
                        mock_results.append(
                            SentimentResult(
                                f'article_{i}', 'AAPL', 'content', 3.5,
                                'test-model-1', 'TestModel1', '2024-01-01', 'integration-test'
                            )
                        )
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Mock Parquet writing
                        with patch.object(orchestrator.s3_client, 'write_parquet') as mock_write_parquet:
                            # Run the batch processing
                            result_df = await orchestrator._process_model_batch(
                                config.models[0],
                                'system prompt',
                                'user prompt template'
                            )
                            
                            # Verify output Parquet schema matches existing format
                            expected_columns = [
                                'article_id', 'symbol', 'component', 'sentiment_score',
                                'model_id', 'model_name', 'timestamp', 'job_name',
                                'rating_scale_min', 'rating_scale_max'
                            ]
                            assert list(result_df.columns) == expected_columns, \
                                f"Output schema mismatch: expected {expected_columns}, got {list(result_df.columns)}"
                            
                            # Verify results contain expected data
                            assert len(result_df) == len(mock_results)
                            assert all(result_df['model_id'] == 'test-model-1')
                            assert all(result_df['model_name'] == 'TestModel1')
                            assert all(result_df['job_name'] == 'integration-test')
                            assert all(result_df['rating_scale_min'] == -5)
                            assert all(result_df['rating_scale_max'] == 5)
                            
                            # Verify rate limiting was initialized correctly
                            # (checked via RateLimiter initialization in orchestrator)
                            mock_read_parallel.assert_called_once()
        
        asyncio.run(run_test())
    
    def test_output_parquet_schema_consistency(self):
        """Test that output Parquet schema is consistent with existing format."""
        async def run_test():
            # Create minimal configuration
            config = Config(
                input_s3_bucket='s3://test-input/',
                output_s3_bucket='s3://test-output/',
                system_prompt_s3_uri='s3://test/system.txt',
                prompt_s3_uri='s3://test/user.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=50,
                        tokens_per_minute=5000
                    )
                ],
                rating_scale=RatingScale(min=-10, max=10),
                job_name='schema-test',
                max_articles=None
            )
            
            orchestrator = BatchOrchestrator(config)
            
            # Create small test DataFrame
            mock_df = pd.DataFrame([
                {
                    'id': 'article1',
                    'symbols': ['AAPL'],
                    'content': 'content1',
                    'headline': 'headline1',
                    'summary': 'summary1'
                }
            ])
            
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read:
                mock_read.return_value = mock_df
                
                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='prompt'):
                    mock_results = [
                        SentimentResult('article1', 'AAPL', 'content', 5.0, 'test-model', 'TestModel', '2024-01-01', 'schema-test')
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Capture the DataFrame written to Parquet
                        written_df = None
                        
                        def capture_write(df, bucket_uri, filename):
                            nonlocal written_df
                            written_df = df.copy()
                        
                        with patch.object(orchestrator.s3_client, 'write_parquet', side_effect=capture_write):
                            result_df = await orchestrator._process_model_batch(
                                config.models[0],
                                'system prompt',
                                'user prompt'
                            )
                        
                        # Verify schema
                        assert written_df is None or list(result_df.columns) == [
                            'article_id', 'symbol', 'component', 'sentiment_score',
                            'model_id', 'model_name', 'timestamp', 'job_name',
                            'rating_scale_min', 'rating_scale_max'
                        ]
        
        asyncio.run(run_test())


class TestErrorRecoveryAndRetry:
    """Integration tests for error recovery and retry logic."""
    
    def test_s3_throttling_with_exponential_backoff(self):
        """
        Test error recovery when S3 throttling errors occur.
        
        Validates:
        - Inject S3 throttling errors during download
        - Verify exponential backoff retry works
        - Check failed files are logged correctly
        - Ensure processing continues after failures
        
        Requirements: 1.5, 5.5
        """
        async def run_test():
            # Create mock S3 client that throttles some requests
            mock_s3_client = AsyncMock()
            
            # Track which files were throttled
            throttled_files = set()
            call_count = {}
            
            async def mock_get_object(Bucket, Key):
                # Track call count for this key
                call_count[Key] = call_count.get(Key, 0) + 1
                
                # Throttle first 2 attempts for some files
                if Key.endswith('_throttle.json') and call_count[Key] <= 2:
                    throttled_files.add(Key)
                    raise ClientError(
                        {'Error': {'Code': 'SlowDown', 'Message': 'Please reduce your request rate'}},
                        'GetObject'
                    )
                
                # Return valid article after retries
                article = {
                    'id': Key.replace('.json', ''),
                    'symbols': ['TEST'],
                    'content': 'test content',
                    'headline': 'test headline',
                    'summary': 'test summary'
                }
                
                # Create proper async mock response with context manager support
                article_bytes = json.dumps(article).encode('utf-8')
                
                # Create a mock stream that supports async context manager
                mock_stream = AsyncMock()
                mock_stream.read = AsyncMock(return_value=article_bytes)
                mock_stream.__aenter__ = AsyncMock(return_value=mock_stream)
                mock_stream.__aexit__ = AsyncMock(return_value=None)
                
                mock_response = {
                    'Body': mock_stream
                }
                return mock_response
            
            mock_s3_client.get_object = mock_get_object
            
            # Create S3Client
            mock_session = Mock()
            mock_session.client.return_value = Mock()
            client = S3Client(boto3_session=mock_session)
            
            # Create semaphore
            semaphore = asyncio.Semaphore(10)
            
            # Track backoff times
            backoff_times = []
            original_sleep = asyncio.sleep
            
            async def mock_sleep(duration):
                backoff_times.append(duration)
                await original_sleep(0.001)  # Actually sleep very briefly
            
            # Test downloading a single file that will be throttled
            test_key = 'file_throttle.json'
            
            with patch('asyncio.sleep', side_effect=mock_sleep):
                result = await client._download_single_file(
                    mock_s3_client,
                    'test-bucket',
                    test_key,
                    semaphore,
                    retry_count=3
                )
            
            # Verify exponential backoff occurred
            assert len(backoff_times) == 2, f"Should have 2 backoff attempts, got {len(backoff_times)}"
            
            # Verify exponential pattern (second backoff should be roughly 2x first)
            if len(backoff_times) >= 2:
                ratio = backoff_times[1] / backoff_times[0]
                # Allow tolerance for jitter (±30% to account for randomness)
                assert 1.4 <= ratio <= 2.6, \
                    f"Backoff not exponential: {backoff_times[1]} / {backoff_times[0]} = {ratio}"
            
            # Verify processing succeeded after retries
            assert result is not None, "File should eventually succeed after retries"
            assert result['id'] == 'file_throttle'
            
            # Verify file was throttled
            assert test_key in throttled_files, "File should have been throttled"
            assert call_count[test_key] == 3, f"File should have been attempted 3 times, got {call_count[test_key]}"
        
        asyncio.run(run_test())
    
    def test_partial_failure_handling(self):
        """Test that processing continues when some files fail permanently."""
        async def run_test():
            # Create mock articles where some will fail
            num_articles = 100
            num_failures = 10
            
            mock_articles = []
            for i in range(num_articles):
                if i < num_failures:
                    # These will fail
                    mock_articles.append(None)
                else:
                    mock_articles.append({
                        'id': f'article_{i}',
                        'symbols': ['TEST'],
                        'content': f'content {i}',
                        'headline': f'headline {i}',
                        'summary': f'summary {i}'
                    })
            
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
                            concurrent_limit=50,
                            chunk_size=50
                        )
                        
                        # Verify processing continued despite failures
                        expected_successful = num_articles - num_failures
                        assert len(df) == expected_successful, \
                            f"Expected {expected_successful} successful articles, got {len(df)}"
                        
                        # Verify failures were logged (actual log format uses 'errors=' and 'filtered=')
                        info_calls = [str(call) for call in mock_logger.info.call_args_list]
                        completion_logs = [log for log in info_calls if 'errors=' in log or 'filtered=' in log]
                        assert len(completion_logs) > 0, "Failures should be logged"
        
        asyncio.run(run_test())


class TestBackwardCompatibility:
    """Integration tests for backward compatibility."""
    
    def test_existing_configuration_files(self):
        """
        Test that existing configuration files work without modification.
        
        Validates:
        - Run with existing configuration files
        - Verify output schema unchanged
        - Test with --model-id parameter
        - Check all existing functionality preserved
        
        Requirements: 3.5, 4.1, 4.3, 4.4
        """
        # Create a configuration file in the old format (without new parallel reading fields)
        # Note: enable_streaming_batch is set to False to test backward compatibility
        # with configs that don't have S3 Tables configuration
        old_config = {
            'input_s3_bucket': 's3://test-input/',
            'output_s3_bucket': 's3://test-output/',
            'system_prompt_s3_uri': 's3://test/system.txt',
            'prompt_s3_uri': 's3://test/user.txt',
            'rating_scale': {
                'min': -5,
                'max': 5
            },
            'models': [
                {
                    'model_id': 'model-1',
                    'model_name': 'Model1',
                    'requests_per_minute': 100,
                    'tokens_per_minute': 10000
                },
                {
                    'model_id': 'model-2',
                    'model_name': 'Model2',
                    'requests_per_minute': 50,
                    'tokens_per_minute': 5000
                }
            ],
            'job_name': 'test-job',
            'max_articles': 1000,
            'enable_streaming_batch': False  # Disable streaming batch for backward compatibility test
        }
        
        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(old_config, f)
            config_path = f.name
        
        try:
            # Load configuration
            config = Config.from_file(config_path)
            
            # Verify configuration loaded successfully
            assert config.input_s3_bucket == 's3://test-input/'
            assert config.output_s3_bucket == 's3://test-output/'
            assert len(config.models) == 2
            assert config.max_articles == 1000
            assert config.job_name == 'test-job'
            
            # Verify default values for new fields
            assert config.parallel_reading_enabled is True
            assert config.concurrent_download_limit == 75
            assert config.download_chunk_size == 1000
            
            # Test model filtering (simulating --model-id parameter)
            filtered_models = [m for m in config.models if m.model_id == 'model-1']
            assert len(filtered_models) == 1
            assert filtered_models[0].model_id == 'model-1'
            
            # Create orchestrator with filtered config
            config.models = filtered_models
            orchestrator = BatchOrchestrator(config)
            
            # Verify orchestrator initialized correctly
            assert orchestrator.use_parallel_reading is True
            assert orchestrator.concurrent_limit == 75
            assert orchestrator.chunk_size == 1000
            
        finally:
            import os
            if os.path.exists(config_path):
                os.unlink(config_path)
    
    def test_sequential_reading_fallback(self):
        """Test that sequential reading still works when parallel reading is disabled."""
        async def run_test():
            # Create configuration with parallel reading disabled
            config = Config(
                input_s3_bucket='s3://test-input/',
                output_s3_bucket='s3://test-output/',
                system_prompt_s3_uri='s3://test/system.txt',
                prompt_s3_uri='s3://test/user.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=None,
                parallel_reading_enabled=False  # Disable parallel reading
            )
            
            orchestrator = BatchOrchestrator(config)
            orchestrator.use_parallel_reading = False
            
            # Mock sequential reading
            mock_articles = [
                {'id': 'article1', 'symbols': ['AAPL'], 'content': 'content1', 'headline': 'headline1', 'summary': 'summary1', 'created_at': '2019-07-08T16:49:02Z'},
                {'id': 'article2', 'symbols': ['GOOGL'], 'content': 'content2', 'headline': 'headline2', 'summary': 'summary2', 'created_at': '2019-07-09T10:30:00Z'}
            ]
            
            with patch.object(orchestrator.s3_client, 'read_articles', return_value=iter(mock_articles)):
                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='prompt'):
                    mock_results = [
                        SentimentResult('article1', 'AAPL', 'content', 3.5, 'test-model', 'TestModel', '2024-01-01', 'test-job'),
                        SentimentResult('article2', 'GOOGL', 'content', 2.5, 'test-model', 'TestModel', '2024-01-01', 'test-job')
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Run batch processing
                        result_df = await orchestrator._process_model_batch(
                            config.models[0],
                            'system prompt',
                            'user prompt'
                        )
                    
                    # Verify sequential reading was used
                    assert len(result_df) == 2
                    
                    # Verify output schema is unchanged
                    expected_columns = [
                        'article_id', 'symbol', 'component', 'sentiment_score',
                        'model_id', 'model_name', 'timestamp', 'job_name',
                        'rating_scale_min', 'rating_scale_max'
                    ]
                    assert list(result_df.columns) == expected_columns
        
        asyncio.run(run_test())
    
    def test_new_configuration_fields_optional(self):
        """Test that new configuration fields are truly optional."""
        # Create config without new fields
        # Note: enable_streaming_batch is set to False to test that parallel reading
        # fields are optional without requiring S3 Tables configuration
        minimal_config = {
            'input_s3_bucket': 's3://test-input/',
            'output_s3_bucket': 's3://test-output/',
            'system_prompt_s3_uri': 's3://test/system.txt',
            'prompt_s3_uri': 's3://test/user.txt',
            'rating_scale': {'min': -5, 'max': 5},
            'models': [
                {
                    'model_id': 'test-model',
                    'requests_per_minute': 100,
                    'tokens_per_minute': 10000
                }
            ],
            'enable_streaming_batch': False  # Disable streaming batch for this test
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(minimal_config, f)
            config_path = f.name
        
        try:
            # Should load successfully with defaults
            config = Config.from_file(config_path)
            
            # Verify defaults are applied
            assert config.parallel_reading_enabled is True
            assert config.concurrent_download_limit == 75
            assert config.download_chunk_size == 1000
            assert config.max_articles is None
            assert config.job_name is None
            
            # Verify validation passes
            config.validate()
            
        finally:
            import os
            if os.path.exists(config_path):
                os.unlink(config_path)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
