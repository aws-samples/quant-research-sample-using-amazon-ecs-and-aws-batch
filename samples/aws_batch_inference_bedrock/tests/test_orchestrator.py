"""Tests for BatchOrchestrator DataFrame workflow functionality."""

import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
import pandas as pd
import pytest
from botocore.exceptions import ClientError

from orchestrator import BatchOrchestrator
from config import Config, ModelConfig, RatingScale
from article_processor import AnalysisTask
from sentiment_analyzer import SentimentResult


class TestDataFrameWorkflow:
    """Unit tests for DataFrame-based processing workflow."""
    
    def test_dataframe_loading_and_task_creation(self):
        """Test DataFrame loading and task creation flow with parallel reading enabled."""
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
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=None
            )
            
            # Create orchestrator
            orchestrator = BatchOrchestrator(config)
            
            # Verify feature flag is enabled by default
            assert orchestrator.use_parallel_reading is True
            assert orchestrator.concurrent_limit == 75
            assert orchestrator.chunk_size == 1000
            
            # Create mock DataFrame
            mock_df = pd.DataFrame({
                'id': ['article1', 'article2'],
                'symbols': [['AAPL'], ['GOOGL', 'MSFT']],
                'content': ['content1', 'content2'],
                'headline': ['headline1', 'headline2'],
                'summary': ['summary1', 'summary2']
            })
            
            # Mock S3 client's read_articles_parallel method
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read_parallel:
                mock_read_parallel.return_value = mock_df
                
                # Mock article processor's process_dataframe method
                mock_tasks = [
                    AnalysisTask('article1', 'AAPL', 'content', 'content1'),
                    AnalysisTask('article1', 'AAPL', 'headline', 'headline1'),
                    AnalysisTask('article2', 'GOOGL', 'content', 'content2'),
                ]
                
                with patch.object(orchestrator.article_processor, 'process_dataframe', return_value=mock_tasks) as mock_process_df:
                    # Mock sentiment analyzer
                    mock_results = [
                        SentimentResult('article1', 'AAPL', 'content', 3.5, 'test-model', 'TestModel', '2024-01-01', 'test-job'),
                        SentimentResult('article1', 'AAPL', 'headline', 4.0, 'test-model', 'TestModel', '2024-01-01', 'test-job'),
                        SentimentResult('article2', 'GOOGL', 'content', 2.5, 'test-model', 'TestModel', '2024-01-01', 'test-job'),
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Run the batch processing
                        result_df = await orchestrator._process_model_batch(
                            config.models[0],
                            'system prompt',
                            'user prompt template'
                        )
                    
                    # Verify read_articles_parallel was called with correct parameters
                    mock_read_parallel.assert_called_once_with(
                        bucket_uri='s3://test-input-bucket/',
                        concurrent_limit=75,
                        chunk_size=1000
                    )
                    
                    # Verify process_dataframe was called
                    mock_process_df.assert_called_once()
                    call_args = mock_process_df.call_args
                    pd.testing.assert_frame_equal(call_args[0][0], mock_df)
                    assert call_args[1]['max_articles'] is None
                    
                    # Verify tasks were analyzed
                    mock_analyze.assert_called_once()
                    
                    # Verify result DataFrame
                    assert len(result_df) == 3
                    assert 'article_id' in result_df.columns
                    assert 'sentiment_score' in result_df.columns
        
        asyncio.run(run_test())
    
    def test_feature_flag_disabled_uses_sequential_reading(self):
        """Test that disabling feature flag falls back to sequential reading."""
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
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=None
            )
            
            # Create orchestrator and disable parallel reading
            orchestrator = BatchOrchestrator(config)
            orchestrator.use_parallel_reading = False
            
            # Mock sequential reading
            mock_articles = [
                {'id': 'article1', 'symbols': ['AAPL'], 'content': 'content1', 'headline': 'headline1', 'summary': 'summary1', 'created_at': '2019-07-08T16:49:02Z'},
                {'id': 'article2', 'symbols': ['GOOGL'], 'content': 'content2', 'headline': 'headline2', 'summary': 'summary2', 'created_at': '2019-07-09T10:30:00Z'}
            ]
            
            with patch.object(orchestrator.s3_client, 'read_articles', return_value=iter(mock_articles)) as mock_read_articles:
                # Mock article processor's process_articles method
                mock_tasks = [
                    AnalysisTask('article1', 'AAPL', 'content', 'content1'),
                    AnalysisTask('article2', 'GOOGL', 'content', 'content2'),
                ]
                
                with patch.object(orchestrator.article_processor, 'process_articles', return_value=iter(mock_tasks)) as mock_process_articles:
                    # Mock sentiment analyzer
                    mock_results = [
                        SentimentResult('article1', 'AAPL', 'content', 3.5, 'test-model', 'TestModel', '2024-01-01', 'test-job'),
                        SentimentResult('article2', 'GOOGL', 'content', 2.5, 'test-model', 'TestModel', '2024-01-01', 'test-job'),
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Run the batch processing
                        result_df = await orchestrator._process_model_batch(
                            config.models[0],
                            'system prompt',
                            'user prompt template'
                        )
                    
                    # Verify read_articles was called (sequential)
                    mock_read_articles.assert_called_once_with('s3://test-input-bucket/')
                    
                    # Verify process_articles was called (not process_dataframe)
                    mock_process_articles.assert_called_once()
                    
                    # Verify result DataFrame
                    assert len(result_df) == 2
        
        asyncio.run(run_test())
    
    def test_memory_usage_tracking(self):
        """Test that memory usage is tracked and logged for DataFrame."""
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
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=None
            )
            
            # Create orchestrator
            orchestrator = BatchOrchestrator(config)
            
            # Create mock DataFrame with known size
            mock_df = pd.DataFrame({
                'id': ['article1', 'article2', 'article3'],
                'symbols': [['AAPL'], ['GOOGL'], ['MSFT']],
                'content': ['content1', 'content2', 'content3'],
                'headline': ['headline1', 'headline2', 'headline3'],
                'summary': ['summary1', 'summary2', 'summary3']
            })
            
            # Mock S3 client's read_articles_parallel method
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read_parallel:
                mock_read_parallel.return_value = mock_df
                
                # Mock article processor
                mock_tasks = [AnalysisTask('article1', 'AAPL', 'content', 'content1')]
                with patch.object(orchestrator.article_processor, 'process_dataframe', return_value=mock_tasks):
                    # Mock sentiment analyzer
                    mock_results = [
                        SentimentResult('article1', 'AAPL', 'content', 3.5, 'test-model', 'TestModel', '2024-01-01', 'test-job')
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Capture log messages
                        with patch('orchestrator.logger') as mock_logger:
                            # Run the batch processing
                            result_df = await orchestrator._process_model_batch(
                                config.models[0],
                                'system prompt',
                                'user prompt template'
                            )
                            
                            # Verify memory usage was logged
                            log_calls = [str(call) for call in mock_logger.info.call_args_list]
                            memory_logged = any('memory_mb' in str(call) for call in log_calls)
                            assert memory_logged, "Memory usage should be logged"
                            
                            # Verify rows were logged
                            rows_logged = any('rows=' in str(call) for call in log_calls)
                            assert rows_logged, "DataFrame rows should be logged"
        
        asyncio.run(run_test())
    
    def test_backward_compatibility_with_max_articles(self):
        """Test backward compatibility when max_articles is specified."""
        async def run_test():
            # Create test configuration with max_articles limit
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
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=5  # Limit to 5 articles
            )
            
            # Create orchestrator
            orchestrator = BatchOrchestrator(config)
            
            # Create mock DataFrame with more articles than limit
            mock_df = pd.DataFrame({
                'id': [f'article{i}' for i in range(10)],
                'symbols': [['AAPL'] for _ in range(10)],
                'content': [f'content{i}' for i in range(10)],
                'headline': [f'headline{i}' for i in range(10)],
                'summary': [f'summary{i}' for i in range(10)]
            })
            
            # Mock S3 client's read_articles_parallel method
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read_parallel:
                mock_read_parallel.return_value = mock_df
                
                # Mock article processor
                mock_tasks = [AnalysisTask(f'article{i}', 'AAPL', 'content', f'content{i}') for i in range(5)]
                with patch.object(orchestrator.article_processor, 'process_dataframe', return_value=mock_tasks) as mock_process_df:
                    # Mock sentiment analyzer
                    mock_results = [
                        SentimentResult(f'article{i}', 'AAPL', 'content', 3.5, 'test-model', 'TestModel', '2024-01-01', 'test-job')
                        for i in range(5)
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Run the batch processing
                        result_df = await orchestrator._process_model_batch(
                            config.models[0],
                            'system prompt',
                            'user prompt template'
                        )
                
                # Verify process_dataframe was called with max_articles parameter
                mock_process_df.assert_called_once()
                call_args = mock_process_df.call_args
                assert call_args[1]['max_articles'] == 5
                
                # Verify result DataFrame has correct number of results
                assert len(result_df) == 5
        
        asyncio.run(run_test())
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty DataFrame (no valid articles)."""
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
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=None
            )
            
            # Create orchestrator
            orchestrator = BatchOrchestrator(config)
            
            # Create empty DataFrame
            empty_df = pd.DataFrame(columns=['id', 'symbols', 'content', 'headline', 'summary', 'created_at'])
            
            # Mock S3 client's read_articles_parallel method
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read_parallel:
                mock_read_parallel.return_value = empty_df
                
                # Mock article processor to return empty task list
                with patch.object(orchestrator.article_processor, 'process_dataframe', return_value=[]):
                    # Run the batch processing
                    result_df = await orchestrator._process_model_batch(
                        config.models[0],
                        'system prompt',
                        'user prompt template'
                    )
                
                # Verify empty DataFrame is returned
                assert len(result_df) == 0
        
        asyncio.run(run_test())
    
    def test_rate_limiting_preserved(self):
        """Test that rate limiting is still applied with DataFrame workflow."""
        async def run_test():
            # Create test configuration with specific rate limits
            config = Config(
                input_s3_bucket='s3://test-input-bucket/',
                output_s3_bucket='s3://test-output-bucket/',
                system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
                prompt_s3_uri='s3://test-bucket/user-prompt.txt',
                models=[
                    ModelConfig(
                        model_id='test-model',
                        model_name='TestModel',
                        requests_per_minute=50,  # Specific rate limit
                        tokens_per_minute=5000
                    )
                ],
                rating_scale=RatingScale(min=-5, max=5),
                job_name='test-job',
                max_articles=None
            )
            
            # Create orchestrator
            orchestrator = BatchOrchestrator(config)
            
            # Create mock DataFrame
            mock_df = pd.DataFrame({
                'id': ['article1'],
                'symbols': [['AAPL']],
                'content': ['content1'],
                'headline': ['headline1'],
                'summary': ['summary1']
            })
            
            # Mock S3 client's read_articles_parallel method
            with patch.object(orchestrator.s3_client, 'read_articles_parallel', new_callable=AsyncMock) as mock_read_parallel:
                mock_read_parallel.return_value = mock_df
                
                # Mock article processor
                mock_tasks = [AnalysisTask('article1', 'AAPL', 'content', 'content1')]
                with patch.object(orchestrator.article_processor, 'process_dataframe', return_value=mock_tasks):
                    # Mock sentiment analyzer
                    mock_results = [
                        SentimentResult('article1', 'AAPL', 'content', 3.5, 'test-model', 'TestModel', '2024-01-01', 'test-job')
                    ]
                    
                    with patch.object(orchestrator, '_analyze_tasks_concurrently', new_callable=AsyncMock) as mock_analyze:
                        mock_analyze.return_value = mock_results
                        
                        # Capture RateLimiter initialization
                        with patch('orchestrator.RateLimiter') as mock_rate_limiter_class:
                            mock_rate_limiter_instance = Mock()
                            mock_rate_limiter_class.return_value = mock_rate_limiter_instance
                            
                            # Run the batch processing
                            result_df = await orchestrator._process_model_batch(
                                config.models[0],
                                'system prompt',
                                'user prompt template'
                            )
                            
                            # Verify RateLimiter was initialized with correct parameters
                            mock_rate_limiter_class.assert_called_once_with(
                                requests_per_minute=50,
                                tokens_per_minute=5000
                            )
        
        asyncio.run(run_test())


class TestConfigurableParameters:
    """Unit tests for configurable parallel reading parameters."""
    
    def test_default_parallel_reading_parameters(self):
        """Test that default parameters are set correctly."""
        config = Config(
            input_s3_bucket='s3://test-input-bucket/',
            output_s3_bucket='s3://test-output-bucket/',
            system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
            prompt_s3_uri='s3://test-bucket/user-prompt.txt',
            models=[],
            rating_scale=RatingScale(min=-5, max=5),
            job_name='test-job',
            max_articles=None
        )
        
        orchestrator = BatchOrchestrator(config)
        
        # Verify default values
        assert orchestrator.use_parallel_reading is True
        assert orchestrator.concurrent_limit == 75
        assert orchestrator.chunk_size == 1000
    
    def test_custom_parallel_reading_parameters(self):
        """Test that custom parameters can be set."""
        config = Config(
            input_s3_bucket='s3://test-input-bucket/',
            output_s3_bucket='s3://test-output-bucket/',
            system_prompt_s3_uri='s3://test-bucket/system-prompt.txt',
            prompt_s3_uri='s3://test-bucket/user-prompt.txt',
            models=[],
            rating_scale=RatingScale(min=-5, max=5),
            job_name='test-job',
            max_articles=None
        )
        
        orchestrator = BatchOrchestrator(config)
        
        # Set custom values
        orchestrator.use_parallel_reading = False
        orchestrator.concurrent_limit = 50
        orchestrator.chunk_size = 500
        
        # Verify custom values
        assert orchestrator.use_parallel_reading is False
        assert orchestrator.concurrent_limit == 50
        assert orchestrator.chunk_size == 500
