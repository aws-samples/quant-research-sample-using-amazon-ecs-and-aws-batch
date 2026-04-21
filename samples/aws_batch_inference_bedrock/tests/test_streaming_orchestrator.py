"""Property-based and unit tests for StreamingBatchOrchestrator."""

import asyncio
from unittest.mock import Mock, patch, AsyncMock
import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings
from botocore.exceptions import ClientError

from orchestrator import StreamingBatchOrchestrator
from config import Config, ModelConfig, RatingScale
from article_processor import AnalysisTask
from sentiment_analyzer import SentimentResult


# Property 2: Sequential batch processing
# **Feature: streaming-batch-s3-tables-output, Property 2: Sequential batch processing**
# **Validates: Requirements 1.2, 1.3**
@given(
    num_articles=st.integers(min_value=10, max_value=50),
    batch_size=st.integers(min_value=5, max_value=15)
)
@settings(max_examples=50, deadline=5000)
def test_property_sequential_batch_processing(num_articles, batch_size):
    """
    Property: For any set of articles divided into batches, batch N+1 should not be
    read from S3 until batch N has completed sentiment analysis and S3 Tables writes.
    
    This test verifies that batches are processed sequentially, not concurrently.
    """
    async def run_test():
        # Track the order of operations
        operation_log = []
        
        # Create test configuration
        config = Config(
            input_s3_bucket='s3://test-input-bucket/',
            output_s3_bucket='s3://test-output-bucket/',
            system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
            prompt_s3_uri='s3://test-bucket/user-prompt.txt',
            models=[
                ModelConfig(
                    model_id='test-model',
                    model_name='TestModel',
                    requests_per_minute=100,
                    tokens_per_minute=10000
                )
            ],
            rating_scale=RatingScale(min=0, max=10),
            job_name='test-job',
            batch_size=batch_size,
            s3_table_name='test-table',
            s3_table_warehouse='s3://test-warehouse/',
            enable_streaming_batch=True,
            write_mode='append'
        )
        
        # Create mock articles
        all_articles = [
            {
                'id': f'article{i}',
                'symbols': ['AAPL'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(num_articles)
        ]
        
        # Calculate expected number of batches
        expected_batches = (num_articles + batch_size - 1) // batch_size
        
        # Create batches
        batches = []
        for i in range(0, num_articles, batch_size):
            batch = all_articles[i:i + batch_size]
            batches.append(batch)
        
        # Mock read_articles_batch to yield batches and log reads
        def mock_read_articles_batch(bucket_uri, batch_size):
            for batch_num, batch in enumerate(batches, 1):
                operation_log.append(('read', batch_num))
                yield batch
        
        # Mock analyze_tasks_concurrently to log analysis
        async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
            batch_num = len([op for op in operation_log if op[0] == 'analyze']) + 1
            operation_log.append(('analyze', batch_num))
            # Return mock results
            return [
                SentimentResult(
                    task.article_id, task.symbol, task.component,
                    5.0, model_id, model_name, '2024-01-01', 'test-job'
                )
                for task in tasks
            ]
        
        # Mock write_results_batch to log writes
        def mock_write_results_batch(results, mode='append'):
            batch_num = len([op for op in operation_log if op[0] == 'write']) + 1
            operation_log.append(('write', batch_num))
            return {'written': len(results), 'failed': 0}
        
        # Mock count_articles
        def mock_count_articles(bucket_uri):
            return num_articles
        
        # Create mock S3TablesWriter
        mock_writer = Mock()
        mock_writer.write_results_batch = mock_write_results_batch
        
        # Create orchestrator with mocked S3TablesWriter
        with patch('orchestrator.S3TablesWriter'):
            orchestrator = StreamingBatchOrchestrator(config)
            
            # Mock the per-model writer creation
            with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                # Apply other mocks
                with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                    with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                        with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                            with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                # Run streaming batch processing
                                stats = await orchestrator._process_model_streaming(
                                    config.models[0],
                                    'system prompt',
                                    'user prompt template'
                                )
        
        # Verify sequential processing: for each batch, operations should be in order
        for batch_num in range(1, expected_batches + 1):
            # Find indices of operations for this batch
            read_idx = next((i for i, op in enumerate(operation_log) if op == ('read', batch_num)), None)
            analyze_idx = next((i for i, op in enumerate(operation_log) if op == ('analyze', batch_num)), None)
            write_idx = next((i for i, op in enumerate(operation_log) if op == ('write', batch_num)), None)
            
            # All operations for this batch should exist
            assert read_idx is not None, f"Batch {batch_num} was not read"
            assert analyze_idx is not None, f"Batch {batch_num} was not analyzed"
            assert write_idx is not None, f"Batch {batch_num} was not written"
            
            # Operations should be in order: read < analyze < write
            assert read_idx < analyze_idx, f"Batch {batch_num}: read should come before analyze"
            assert analyze_idx < write_idx, f"Batch {batch_num}: analyze should come before write"
            
            # If there's a next batch, its read should come after current batch's write
            if batch_num < expected_batches:
                next_read_idx = next((i for i, op in enumerate(operation_log) if op == ('read', batch_num + 1)), None)
                if next_read_idx is not None:
                    assert write_idx < next_read_idx, f"Batch {batch_num + 1} read should come after batch {batch_num} write"
    
    asyncio.run(run_test())


# Property 3: Complete article processing
# **Feature: streaming-batch-s3-tables-output, Property 3: Complete article processing**
# **Validates: Requirements 1.4**
@given(
    total_articles=st.integers(min_value=1, max_value=50),
    batch_size=st.integers(min_value=1, max_value=15)
)
@settings(max_examples=50, deadline=5000)
def test_property_complete_article_processing(total_articles, batch_size):
    """
    Property: For any total number of articles T and batch size B, the sum of articles
    processed across all batches should equal T.
    
    This test verifies that all articles are processed exactly once.
    """
    async def run_test():
        # Track processed articles
        processed_article_ids = set()
        
        # Create test configuration
        config = Config(
            input_s3_bucket='s3://test-input-bucket/',
            output_s3_bucket='s3://test-output-bucket/',
            system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
            prompt_s3_uri='s3://test-bucket/user-prompt.txt',
            models=[
                ModelConfig(
                    model_id='test-model',
                    model_name='TestModel',
                    requests_per_minute=100,
                    tokens_per_minute=10000
                )
            ],
            rating_scale=RatingScale(min=0, max=10),
            job_name='test-job',
            batch_size=batch_size,
            s3_table_name='test-table',
            s3_table_warehouse='s3://test-warehouse/',
            enable_streaming_batch=True,
            write_mode='append'
        )
        
        # Create mock articles with unique IDs
        all_articles = [
            {
                'id': f'article{i}',
                'symbols': ['AAPL'],
                'content': f'content{i}',
                'headline': f'headline{i}',
                'summary': f'summary{i}'
            }
            for i in range(total_articles)
        ]
        
        # Create batches
        batches = []
        for i in range(0, total_articles, batch_size):
            batch = all_articles[i:i + batch_size]
            batches.append(batch)
        
        # Mock read_articles_batch to yield batches
        def mock_read_articles_batch(bucket_uri, batch_size):
            for batch in batches:
                yield batch
        
        # Mock analyze_tasks_concurrently to track processed articles
        async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
            # Track article IDs from tasks
            for task in tasks:
                processed_article_ids.add(task.article_id)
            
            # Return mock results
            return [
                SentimentResult(
                    task.article_id, task.symbol, task.component,
                    5.0, model_id, model_name, '2024-01-01', 'test-job'
                )
                for task in tasks
            ]
        
        # Mock write_results_batch
        def mock_write_results_batch(results, mode='append'):
            return {'written': len(results), 'failed': 0}
        
        # Mock count_articles
        def mock_count_articles(bucket_uri):
            return total_articles
        
        # Create mock S3TablesWriter
        mock_writer = Mock()
        mock_writer.write_results_batch = mock_write_results_batch
        
        # Create orchestrator with mocked S3TablesWriter
        with patch('orchestrator.S3TablesWriter'):
            orchestrator = StreamingBatchOrchestrator(config)
            
            # Mock the per-model writer creation
            with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                # Apply other mocks
                with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                    with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                        with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                            with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                # Run streaming batch processing
                                stats = await orchestrator._process_model_streaming(
                                    config.models[0],
                                    'system prompt',
                                    'user prompt template'
                                )
        
        # Verify all articles were processed exactly once
        expected_article_ids = {f'article{i}' for i in range(total_articles)}
        assert processed_article_ids == expected_article_ids, \
            f"Expected {len(expected_article_ids)} unique articles, got {len(processed_article_ids)}"
        
        # Verify stats match
        assert stats['total_articles'] == total_articles, \
            f"Stats should show {total_articles} articles processed, got {stats['total_articles']}"
    
    asyncio.run(run_test())


# Property 10: Progress logging frequency
# **Feature: streaming-batch-s3-tables-output, Property 10: Progress logging frequency**
# **Validates: Requirements 9.2**
@given(
    num_batches=st.integers(min_value=1, max_value=5),
    articles_per_batch=st.integers(min_value=1, max_value=10)
)
@settings(max_examples=50, deadline=5000)
def test_property_progress_logging_frequency(num_batches, articles_per_batch):
    """
    Property: For any streaming batch processing run, progress logs should be
    generated after each batch completes.
    
    This test verifies that progress is logged at the correct frequency.
    """
    async def run_test():
        # Track log calls
        log_calls = []
        
        # Create test configuration
        config = Config(
            input_s3_bucket='s3://test-input-bucket/',
            output_s3_bucket='s3://test-output-bucket/',
            system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
            prompt_s3_uri='s3://test-bucket/user-prompt.txt',
            models=[
                ModelConfig(
                    model_id='test-model',
                    model_name='TestModel',
                    requests_per_minute=100,
                    tokens_per_minute=10000
                )
            ],
            rating_scale=RatingScale(min=0, max=10),
            job_name='test-job',
            batch_size=articles_per_batch,
            s3_table_name='test-table',
            s3_table_warehouse='s3://test-warehouse/',
            enable_streaming_batch=True,
            write_mode='append'
        )
        
        # Create mock articles
        all_articles = []
        for batch_num in range(num_batches):
            for i in range(articles_per_batch):
                article_id = f'article_b{batch_num}_a{i}'
                all_articles.append({
                    'id': article_id,
                    'symbols': ['AAPL'],
                    'content': f'content{article_id}',
                    'headline': f'headline{article_id}',
                    'summary': f'summary{article_id}'
                })
        
        # Create batches
        batches = []
        for i in range(0, len(all_articles), articles_per_batch):
            batch = all_articles[i:i + articles_per_batch]
            batches.append(batch)
        
        # Mock read_articles_batch to yield batches
        def mock_read_articles_batch(bucket_uri, batch_size):
            for batch in batches:
                yield batch
        
        # Mock analyze_tasks_concurrently
        async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
            return [
                SentimentResult(
                    task.article_id, task.symbol, task.component,
                    5.0, model_id, model_name, '2024-01-01', 'test-job'
                )
                for task in tasks
            ]
        
        # Mock write_results_batch
        def mock_write_results_batch(results, mode='append'):
            return {'written': len(results), 'failed': 0}
        
        # Mock count_articles
        def mock_count_articles(bucket_uri):
            return len(all_articles)
        
        # Mock logger to capture log calls
        def mock_log_info(message, *args, **kwargs):
            log_calls.append(str(message))
        
        # Create mock S3TablesWriter
        mock_writer = Mock()
        mock_writer.write_results_batch = mock_write_results_batch
        
        # Create orchestrator with mocked S3TablesWriter
        with patch('orchestrator.S3TablesWriter'):
            orchestrator = StreamingBatchOrchestrator(config)
            
            # Mock the per-model writer creation
            with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                # Apply other mocks
                with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                    with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                        with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                            with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                with patch('orchestrator.logger') as mock_logger:
                                    mock_logger.info = mock_log_info
                                    
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
        
        # Count batch completion logs
        # Look for logs that contain "Batch progress" pattern (updated log format)
        batch_completion_logs = [
            log for log in log_calls 
            if 'batch progress' in log.lower() or ('completed' in log.lower() and 'batch' in log.lower())
        ]
        
        # Should have at least one log per batch
        assert len(batch_completion_logs) >= num_batches, \
            f"Expected at least {num_batches} batch completion logs, got {len(batch_completion_logs)}"
    
    asyncio.run(run_test())



# Unit tests for StreamingBatchOrchestrator

class TestStreamingBatchOrchestrator:
    """Unit tests for streaming batch orchestration logic."""
    
    def test_batch_loop_execution(self):
        """Test that batch loop executes correctly with mock S3 and S3 Tables."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=10,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles in 3 batches
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(10)]
            batch2 = [{'id': f'article{i}', 'symbols': ['GOOGL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(10, 20)]
            batch3 = [{'id': f'article{i}', 'symbols': ['MSFT'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(20, 25)]
            
            batches = [batch1, batch2, batch3]
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock analyze_tasks_concurrently
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                return [
                    SentimentResult(
                        task.article_id, task.symbol, task.component,
                        5.0, model_id, model_name, '2024-01-01', 'test-job'
                    )
                    for task in tasks
                ]
            
            # Mock write_results_batch
            write_calls = []
            def mock_write_results_batch(results, mode='append'):
                write_calls.append(len(results))
                return {'written': len(results), 'failed': 0}
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 25
            
            # Create mock S3TablesWriter
            mock_writer = Mock()
            mock_writer.write_results_batch = mock_write_results_batch
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                
                # Mock the per-model writer creation
                with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                    # Apply mocks
                    with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                        with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                            with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify batch loop executed 3 times
            assert len(write_calls) == 3, f"Expected 3 write calls, got {len(write_calls)}"
            
            # Verify stats
            assert stats['total_articles'] == 25
            assert stats['batches'] == 3
        
        asyncio.run(run_test())
    
    def test_progress_tracking_across_batches(self):
        """Test that progress is tracked correctly across batches."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=5,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(5)]
            batch2 = [{'id': f'article{i}', 'symbols': ['GOOGL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(5, 10)]
            
            batches = [batch1, batch2]
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock analyze_tasks_concurrently
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                # Return some successful and some failed results
                results = []
                for i, task in enumerate(tasks):
                    score = 5.0 if i % 2 == 0 else None  # Half succeed, half fail
                    results.append(
                        SentimentResult(
                            task.article_id, task.symbol, task.component,
                            score, model_id, model_name, '2024-01-01', 'test-job'
                        )
                    )
                return results
            
            # Mock write_results_batch
            def mock_write_results_batch(results, mode='append'):
                return {'written': len(results), 'failed': 0}
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 10
            
            # Create mock S3TablesWriter
            mock_writer = Mock()
            mock_writer.write_results_batch = mock_write_results_batch
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                
                # Mock the per-model writer creation
                with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                    # Apply mocks
                    with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                        with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                            with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify cumulative statistics
            assert stats['total_articles'] == 10
            assert stats['batches'] == 2
            # Each article generates 3 tasks (content, headline, summary)
            # Half of tasks succeed, half fail
            assert stats['successful'] > 0
            assert stats['failed'] > 0
        
        asyncio.run(run_test())
    
    def test_error_handling_for_batch_failures(self):
        """Test that batch failures are handled gracefully."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=5,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(5)]
            
            batches = [batch1]
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock analyze_tasks_concurrently to raise exception
            call_count = [0]
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                call_count[0] += 1
                # Return results (no exception - we want to test graceful handling)
                return [
                    SentimentResult(
                        task.article_id, task.symbol, task.component,
                        None, model_id, model_name, '2024-01-01', 'test-job'
                    )
                    for task in tasks
                ]
            
            # Mock write_results_batch
            def mock_write_results_batch(results, mode='append'):
                return {'written': len(results), 'failed': 0}
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 5
            
            # Create mock S3TablesWriter
            mock_writer = Mock()
            mock_writer.write_results_batch = mock_write_results_batch
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                
                # Mock the per-model writer creation
                with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                    # Apply mocks
                    with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                        with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                            with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify processing completed despite failures
            assert call_count[0] == 1
            assert stats['total_articles'] == 5
        
        asyncio.run(run_test())
    
    def test_statistics_aggregation(self):
        """Test that statistics are aggregated correctly across batches."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=3,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles in 3 batches
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(3)]
            batch2 = [{'id': f'article{i}', 'symbols': ['GOOGL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(3, 6)]
            batch3 = [{'id': f'article{i}', 'symbols': ['MSFT'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(6, 9)]
            
            batches = [batch1, batch2, batch3]
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock analyze_tasks_concurrently
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                return [
                    SentimentResult(
                        task.article_id, task.symbol, task.component,
                        5.0, model_id, model_name, '2024-01-01', 'test-job'
                    )
                    for task in tasks
                ]
            
            # Mock write_results_batch
            def mock_write_results_batch(results, mode='append'):
                return {'written': len(results), 'failed': 0}
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 9
            
            # Create mock S3TablesWriter
            mock_writer = Mock()
            mock_writer.write_results_batch = mock_write_results_batch
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                
                # Mock the per-model writer creation
                with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer):
                    # Apply mocks
                    with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                        with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                            with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify aggregated statistics
            assert stats['total_articles'] == 9
            assert stats['batches'] == 3
            # Each article generates 3 tasks (content, headline, summary)
            assert stats['total_tasks'] == 9 * 3
            assert stats['successful'] == 9 * 3
            assert stats['failed'] == 0
        
        asyncio.run(run_test())
    
    def test_append_vs_upsert_mode_selection(self):
        """Test that append vs upsert mode is correctly selected."""
        async def run_test():
            # Test append mode
            config_append = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=5,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(5)]
            
            batches = [batch1]
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock analyze_tasks_concurrently
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                return [
                    SentimentResult(
                        task.article_id, task.symbol, task.component,
                        5.0, model_id, model_name, '2024-01-01', 'test-job'
                    )
                    for task in tasks
                ]
            
            # Mock write_results_batch to capture mode
            write_mode_used = []
            def mock_write_results_batch(results, mode='append'):
                write_mode_used.append(mode)
                return {'written': len(results), 'failed': 0}
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 5
            
            # Create mock S3TablesWriter
            mock_writer_append = Mock()
            mock_writer_append.write_results_batch = mock_write_results_batch
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config_append)
                
                # Mock the per-model writer creation
                with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer_append):
                    # Apply mocks
                    with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                        with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                            with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                    # Run streaming batch processing
                                    await orchestrator._process_model_streaming(
                                        config_append.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify append mode was used
            assert len(write_mode_used) == 1
            assert write_mode_used[0] == 'append'
            
            # Test upsert mode
            config_upsert = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=5,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='upsert'
            )
            
            write_mode_used = []
            
            # Create mock S3TablesWriter
            mock_writer_upsert = Mock()
            mock_writer_upsert.write_results_batch = mock_write_results_batch
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config_upsert)
                
                # Mock the per-model writer creation
                with patch.object(orchestrator, '_create_s3_tables_writer_for_model', return_value=mock_writer_upsert):
                    # Apply mocks
                    with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                        with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                            with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                                with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                    # Run streaming batch processing
                                    await orchestrator._process_model_streaming(
                                        config_upsert.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify upsert mode was used
            assert len(write_mode_used) == 1
            assert write_mode_used[0] == 'upsert'
        
        asyncio.run(run_test())


class TestStreamingLogging:
    """Unit tests for streaming batch logging functionality.
    
    Tests for Requirements 9.1, 9.2, 9.3, 9.4, 9.5.
    """
    
    def test_streaming_start_logs_total_articles_and_batch_size(self):
        """Test that streaming start logs total articles and batch size (Requirement 9.1)."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=100,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(50)]
            
            batches = [batch1]
            log_calls = []
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 50
            
            # Mock analyze_tasks_concurrently
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                return [
                    SentimentResult(
                        task.article_id, task.symbol, task.component,
                        5.0, model_id, model_name, '2024-01-01', 'test-job'
                    )
                    for task in tasks
                ]
            
            # Mock write_results_batch
            def mock_write_results_batch(results, mode='append'):
                return {'written': len(results), 'failed': 0}
            
            # Mock logger to capture log calls
            def mock_log_info(message, *args, **kwargs):
                log_calls.append(str(message))
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                orchestrator.s3_tables_writer = Mock()
                orchestrator.s3_tables_writer.write_results_batch = mock_write_results_batch
                
                # Apply mocks
                with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                    with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                        with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                            with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                with patch('orchestrator.logger') as mock_logger:
                                    mock_logger.info = mock_log_info
                                    
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Verify streaming start log contains total_articles and batch_size
            streaming_start_logged = False
            for log in log_calls:
                if 'Streaming batch start' in log:
                    streaming_start_logged = True
                    assert 'total_articles=' in log, "Log should contain total_articles"
                    assert 'batch_size=' in log, "Log should contain batch_size"
                    assert 'model_id=' in log, "Log should contain model_id"
                    break
            
            assert streaming_start_logged, "Streaming start should be logged"
        
        asyncio.run(run_test())
    
    def test_batch_progress_logging(self):
        """Test that progress is logged after each batch (Requirement 9.2)."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=10,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles in 3 batches
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(10)]
            batch2 = [{'id': f'article{i}', 'symbols': ['GOOGL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(10, 20)]
            batch3 = [{'id': f'article{i}', 'symbols': ['MSFT'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(20, 30)]
            
            batches = [batch1, batch2, batch3]
            log_calls = []
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 30
            
            # Mock analyze_tasks_concurrently
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                return [
                    SentimentResult(
                        task.article_id, task.symbol, task.component,
                        5.0, model_id, model_name, '2024-01-01', 'test-job'
                    )
                    for task in tasks
                ]
            
            # Mock write_results_batch
            def mock_write_results_batch(results, mode='append'):
                return {'written': len(results), 'failed': 0}
            
            # Mock logger to capture log calls
            def mock_log_info(message, *args, **kwargs):
                log_calls.append(str(message))
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                orchestrator.s3_tables_writer = Mock()
                orchestrator.s3_tables_writer.write_results_batch = mock_write_results_batch
                
                # Apply mocks
                with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                    with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                        with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                            with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                with patch('orchestrator.logger') as mock_logger:
                                    mock_logger.info = mock_log_info
                                    
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Count batch progress logs
            batch_progress_logs = [log for log in log_calls if 'Batch progress' in log]
            
            # Should have 3 batch progress logs (one per batch)
            assert len(batch_progress_logs) == 3, f"Expected 3 batch progress logs, got {len(batch_progress_logs)}"
            
            # Verify each progress log contains required information
            for log in batch_progress_logs:
                assert 'batch=' in log, "Progress log should contain batch number"
                assert 'batch_time=' in log, "Progress log should contain batch time"
                assert 'batch_articles=' in log, "Progress log should contain batch articles"
                assert 'total_articles=' in log, "Progress log should contain cumulative articles"
        
        asyncio.run(run_test())
    
    def test_streaming_completion_logging(self):
        """Test that streaming completion logs total time and success/failure counts (Requirement 9.4)."""
        async def run_test():
            # Create test configuration
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=100,
                        tokens_per_minute=10000
                    )
                ],
                rating_scale=RatingScale(min=0, max=10),
                job_name='test-job',
                batch_size=10,
                s3_table_name='test-table',
                s3_table_warehouse='s3://test-warehouse/',
                enable_streaming_batch=True,
                write_mode='append'
            )
            
            # Create mock articles
            batch1 = [{'id': f'article{i}', 'symbols': ['AAPL'], 'content': f'content{i}', 
                      'headline': f'headline{i}', 'summary': f'summary{i}'} for i in range(10)]
            
            batches = [batch1]
            log_calls = []
            
            # Mock read_articles_batch
            def mock_read_articles_batch(bucket_uri, batch_size):
                for batch in batches:
                    yield batch
            
            # Mock count_articles
            def mock_count_articles(bucket_uri):
                return 10
            
            # Mock analyze_tasks_concurrently - return some successful and some failed
            async def mock_analyze_tasks(tasks, sentiment_analyzer, model_id, model_name):
                results = []
                for i, task in enumerate(tasks):
                    score = 5.0 if i % 2 == 0 else None  # Half succeed, half fail
                    results.append(
                        SentimentResult(
                            task.article_id, task.symbol, task.component,
                            score, model_id, model_name, '2024-01-01', 'test-job'
                        )
                    )
                return results
            
            # Mock write_results_batch
            def mock_write_results_batch(results, mode='append'):
                return {'written': len(results), 'failed': 0}
            
            # Mock logger to capture log calls
            def mock_log_info(message, *args, **kwargs):
                log_calls.append(str(message))
            
            # Create orchestrator with mocked S3TablesWriter
            with patch('orchestrator.S3TablesWriter'):
                orchestrator = StreamingBatchOrchestrator(config)
                orchestrator.s3_tables_writer = Mock()
                orchestrator.s3_tables_writer.write_results_batch = mock_write_results_batch
                
                # Apply mocks
                with patch.object(orchestrator.s3_client, 'read_articles_batch', side_effect=mock_read_articles_batch):
                    with patch.object(orchestrator.s3_client, 'count_articles', side_effect=mock_count_articles):
                        with patch.object(orchestrator, '_analyze_tasks_concurrently', side_effect=mock_analyze_tasks):
                            with patch.object(orchestrator.s3_client, 'read_prompt_template', return_value='test prompt'):
                                with patch('orchestrator.logger') as mock_logger:
                                    mock_logger.info = mock_log_info
                                    
                                    # Run streaming batch processing
                                    stats = await orchestrator._process_model_streaming(
                                        config.models[0],
                                        'system prompt',
                                        'user prompt template'
                                    )
            
            # Find streaming completion log
            completion_logs = [log for log in log_calls if 'Streaming batch completion' in log]
            
            assert len(completion_logs) == 1, "Should have exactly one completion log"
            
            completion_log = completion_logs[0]
            
            # Verify completion log contains required information
            assert 'total_time=' in completion_log, "Completion log should contain total_time"
            assert 'successful=' in completion_log, "Completion log should contain successful count"
            assert 'failed=' in completion_log, "Completion log should contain failed count"
            assert 'total_articles=' in completion_log, "Completion log should contain total_articles"
            assert 'success_rate=' in completion_log, "Completion log should contain success_rate"
        
        asyncio.run(run_test())
