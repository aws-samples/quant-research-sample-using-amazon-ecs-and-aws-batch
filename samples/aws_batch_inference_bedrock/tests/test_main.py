"""Tests for main.py CLI model filtering functionality."""

import json
import sys
import tempfile
from unittest.mock import Mock, patch, MagicMock
import pytest

from config import Config, ModelConfig, RatingScale


class TestCLIModelFiltering:
    """Unit tests for CLI --model-id parameter filtering."""
    
    def test_model_id_parameter_filters_correctly(self):
        """Test --model-id parameter filters to single model correctly."""
        # Create test configuration with multiple models
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'model-2',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                },
                {
                    'model_id': 'model-3',
                    'requests_per_minute': 30,
                    'tokens_per_minute': 3000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path, '--model-id', 'model-2']
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_orchestrator_class:
                    with patch('main.configure_logging'):
                        # Import and run main
                        from main import main
                        
                        # Mock orchestrator.run() to prevent actual execution
                        mock_orchestrator_instance = Mock()
                        mock_orchestrator_class.return_value = mock_orchestrator_instance
                        
                        # Run main
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Verify successful exit
                        assert exc_info.value.code == 0
                        
                        # Verify orchestrator was initialized with filtered config
                        mock_orchestrator_class.assert_called_once()
                        filtered_config = mock_orchestrator_class.call_args[0][0]
                        
                        # Verify only model-2 remains
                        assert len(filtered_config.models) == 1
                        assert filtered_config.models[0].model_id == 'model-2'
                        assert filtered_config.models[0].requests_per_minute == 20
                        assert filtered_config.models[0].tokens_per_minute == 2000
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_error_handling_for_invalid_model_id(self):
        """Test error handling when invalid model ID is specified."""
        # Create test configuration with multiple models
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'model-2',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments with invalid model ID
            test_args = ['main.py', '--config', tmp_path, '--model-id', 'invalid-model']
            
            with patch('sys.argv', test_args):
                with patch('main.configure_logging'):
                    # Import and run main
                    from main import main
                    
                    # Run main - should exit with error code 1
                    with pytest.raises(SystemExit) as exc_info:
                        main()
                    
                    # Verify error exit code
                    assert exc_info.value.code == 1
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_backward_compatibility_without_model_id(self):
        """Test backward compatibility when --model-id is not specified."""
        # Create test configuration with multiple models
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'model-2',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                },
                {
                    'model_id': 'model-3',
                    'requests_per_minute': 30,
                    'tokens_per_minute': 3000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments WITHOUT --model-id
            test_args = ['main.py', '--config', tmp_path]
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_orchestrator_class:
                    with patch('main.configure_logging'):
                        # Import and run main
                        from main import main
                        
                        # Mock orchestrator.run() to prevent actual execution
                        mock_orchestrator_instance = Mock()
                        mock_orchestrator_class.return_value = mock_orchestrator_instance
                        
                        # Run main
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Verify successful exit
                        assert exc_info.value.code == 0
                        
                        # Verify orchestrator was initialized with all models
                        mock_orchestrator_class.assert_called_once()
                        config = mock_orchestrator_class.call_args[0][0]
                        
                        # Verify all 3 models are present
                        assert len(config.models) == 3
                        assert config.models[0].model_id == 'model-1'
                        assert config.models[1].model_id == 'model-2'
                        assert config.models[2].model_id == 'model-3'
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_model_filtering_happens_before_orchestrator_init(self):
        """Test that model filtering occurs before orchestrator initialization."""
        # Create test configuration with multiple models
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'model-2',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path, '--model-id', 'model-1']
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_orchestrator_class:
                    with patch('main.configure_logging'):
                        # Import and run main
                        from main import main
                        
                        # Mock orchestrator.run() to prevent actual execution
                        mock_orchestrator_instance = Mock()
                        mock_orchestrator_class.return_value = mock_orchestrator_instance
                        
                        # Run main
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Verify successful exit
                        assert exc_info.value.code == 0
                        
                        # Verify orchestrator was called exactly once
                        mock_orchestrator_class.assert_called_once()
                        
                        # Get the config passed to orchestrator
                        config_passed = mock_orchestrator_class.call_args[0][0]
                        
                        # Verify the config already has filtered models
                        # This proves filtering happened BEFORE orchestrator init
                        assert len(config_passed.models) == 1
                        assert config_passed.models[0].model_id == 'model-1'
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_filtered_model_count_logging(self):
        """Test that filtered model count is logged correctly."""
        # Create test configuration with multiple models
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'model-2',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                },
                {
                    'model_id': 'model-3',
                    'requests_per_minute': 30,
                    'tokens_per_minute': 3000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path, '--model-id', 'model-2']
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_orchestrator_class:
                    with patch('main.configure_logging'):
                        # Mock logging.getLogger to capture log calls
                        with patch('logging.getLogger') as mock_get_logger:
                            mock_logger = Mock()
                            mock_get_logger.return_value = mock_logger
                            
                            # Import and run main
                            from main import main
                            
                            # Mock orchestrator.run() to prevent actual execution
                            mock_orchestrator_instance = Mock()
                            mock_orchestrator_class.return_value = mock_orchestrator_instance
                            
                            # Run main
                            with pytest.raises(SystemExit) as exc_info:
                                main()
                            
                            # Verify successful exit
                            assert exc_info.value.code == 0
                            
                            # Verify logging occurred
                            log_calls = [str(call) for call in mock_logger.info.call_args_list]
                            
                            # Check for filtering log message
                            filtering_logged = any('Filtering configuration for model' in str(call) for call in log_calls)
                            assert filtering_logged, "Should log filtering message"
                            
                            # Check for filtered count log message
                            count_logged = any('Filtered to 1 model (from 3 total models)' in str(call) for call in log_calls)
                            assert count_logged, "Should log filtered model count"
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_multiple_models_with_same_prefix(self):
        """Test filtering works correctly when model IDs have similar prefixes."""
        # Create test configuration with models that have similar names
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'claude-v1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'claude-v2',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                },
                {
                    'model_id': 'claude-v2-extended',
                    'requests_per_minute': 30,
                    'tokens_per_minute': 3000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments - filter for exact match
            test_args = ['main.py', '--config', tmp_path, '--model-id', 'claude-v2']
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_orchestrator_class:
                    with patch('main.configure_logging'):
                        # Import and run main
                        from main import main
                        
                        # Mock orchestrator.run() to prevent actual execution
                        mock_orchestrator_instance = Mock()
                        mock_orchestrator_class.return_value = mock_orchestrator_instance
                        
                        # Run main
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Verify successful exit
                        assert exc_info.value.code == 0
                        
                        # Verify orchestrator was initialized with exact match only
                        mock_orchestrator_class.assert_called_once()
                        filtered_config = mock_orchestrator_class.call_args[0][0]
                        
                        # Verify only claude-v2 remains (not claude-v2-extended)
                        assert len(filtered_config.models) == 1
                        assert filtered_config.models[0].model_id == 'claude-v2'
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_case_sensitive_model_filtering(self):
        """Test that model ID filtering is case-sensitive."""
        # Create test configuration
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'Model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                },
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 20,
                    'tokens_per_minute': 2000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments - use lowercase
            test_args = ['main.py', '--config', tmp_path, '--model-id', 'model-1']
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_orchestrator_class:
                    with patch('main.configure_logging'):
                        # Import and run main
                        from main import main
                        
                        # Mock orchestrator.run() to prevent actual execution
                        mock_orchestrator_instance = Mock()
                        mock_orchestrator_class.return_value = mock_orchestrator_instance
                        
                        # Run main
                        with pytest.raises(SystemExit) as exc_info:
                            main()
                        
                        # Verify successful exit
                        assert exc_info.value.code == 0
                        
                        # Verify only lowercase model-1 was selected
                        mock_orchestrator_class.assert_called_once()
                        filtered_config = mock_orchestrator_class.call_args[0][0]
                        
                        assert len(filtered_config.models) == 1
                        assert filtered_config.models[0].model_id == 'model-1'
                        assert filtered_config.models[0].requests_per_minute == 20
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


if __name__ == "__main__":
    import sys
    
    # Run tests
    sys.exit(pytest.main([__file__, "-v"]))


class TestOrchestratorSelection:
    """Unit tests for orchestrator selection based on enable_streaming_batch flag."""
    
    def test_streaming_orchestrator_used_when_flag_enabled(self):
        """Test StreamingBatchOrchestrator is used when enable_streaming_batch is True."""
        # Create test configuration with streaming batch enabled
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                }
            ],
            'enable_streaming_batch': True,
            's3_table_name': 'test-sentiment-results',
            's3_table_warehouse': 's3://test-warehouse/',
            'batch_size': 1000,
            'write_mode': 'upsert'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path]
            
            with patch('sys.argv', test_args):
                with patch('main.StreamingBatchOrchestrator') as mock_streaming_class:
                    with patch('main.BatchOrchestrator') as mock_batch_class:
                        with patch('main.configure_logging'):
                            # Import and run main
                            from main import main
                            
                            # Mock orchestrator.run() to prevent actual execution
                            mock_streaming_instance = Mock()
                            mock_streaming_class.return_value = mock_streaming_instance
                            
                            # Run main
                            with pytest.raises(SystemExit) as exc_info:
                                main()
                            
                            # Verify successful exit
                            assert exc_info.value.code == 0
                            
                            # Verify StreamingBatchOrchestrator was used
                            mock_streaming_class.assert_called_once()
                            mock_batch_class.assert_not_called()
                            
                            # Verify config passed to streaming orchestrator
                            config_passed = mock_streaming_class.call_args[0][0]
                            assert config_passed.enable_streaming_batch is True
                            assert config_passed.s3_table_name == 'test-sentiment-results'
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_batch_orchestrator_used_when_flag_disabled(self):
        """Test BatchOrchestrator is used when enable_streaming_batch is False."""
        # Create test configuration with streaming batch disabled
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path]
            
            with patch('sys.argv', test_args):
                with patch('main.StreamingBatchOrchestrator') as mock_streaming_class:
                    with patch('main.BatchOrchestrator') as mock_batch_class:
                        with patch('main.configure_logging'):
                            # Import and run main
                            from main import main
                            
                            # Mock orchestrator.run() to prevent actual execution
                            mock_batch_instance = Mock()
                            mock_batch_class.return_value = mock_batch_instance
                            
                            # Run main
                            with pytest.raises(SystemExit) as exc_info:
                                main()
                            
                            # Verify successful exit
                            assert exc_info.value.code == 0
                            
                            # Verify BatchOrchestrator was used
                            mock_batch_class.assert_called_once()
                            mock_streaming_class.assert_not_called()
                            
                            # Verify config passed to batch orchestrator
                            config_passed = mock_batch_class.call_args[0][0]
                            assert config_passed.enable_streaming_batch is False
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_backward_compatibility_defaults_to_streaming(self):
        """Test backward compatibility - streaming batch is enabled by default."""
        # Create test configuration without explicit enable_streaming_batch
        # but with required s3_table fields (since streaming is default)
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                }
            ],
            # Required when streaming is enabled (default)
            's3_table_name': 'test-sentiment-results',
            's3_table_warehouse': 's3://test-warehouse/'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path]
            
            with patch('sys.argv', test_args):
                with patch('main.StreamingBatchOrchestrator') as mock_streaming_class:
                    with patch('main.BatchOrchestrator') as mock_batch_class:
                        with patch('main.configure_logging'):
                            # Import and run main
                            from main import main
                            
                            # Mock orchestrator.run() to prevent actual execution
                            mock_streaming_instance = Mock()
                            mock_streaming_class.return_value = mock_streaming_instance
                            
                            # Run main
                            with pytest.raises(SystemExit) as exc_info:
                                main()
                            
                            # Verify successful exit
                            assert exc_info.value.code == 0
                            
                            # Verify StreamingBatchOrchestrator was used (default)
                            mock_streaming_class.assert_called_once()
                            mock_batch_class.assert_not_called()
                            
                            # Verify default values
                            config_passed = mock_streaming_class.call_args[0][0]
                            assert config_passed.enable_streaming_batch is True
                            assert config_passed.batch_size == 1000  # default
                            assert config_passed.write_mode == 'upsert'  # default
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_orchestrator_selection_logs_correctly(self):
        """Test that orchestrator selection is logged correctly."""
        # Create test configuration with streaming batch enabled
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                }
            ],
            'enable_streaming_batch': True,
            's3_table_name': 'test-sentiment-results',
            's3_table_warehouse': 's3://test-warehouse/',
            'batch_size': 500,
            'write_mode': 'append'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path]
            
            with patch('sys.argv', test_args):
                with patch('main.StreamingBatchOrchestrator') as mock_streaming_class:
                    with patch('main.configure_logging'):
                        # Mock logging.getLogger to capture log calls
                        with patch('logging.getLogger') as mock_get_logger:
                            mock_logger = Mock()
                            mock_get_logger.return_value = mock_logger
                            
                            # Import and run main
                            from main import main
                            
                            # Mock orchestrator.run() to prevent actual execution
                            mock_streaming_instance = Mock()
                            mock_streaming_class.return_value = mock_streaming_instance
                            
                            # Run main
                            with pytest.raises(SystemExit) as exc_info:
                                main()
                            
                            # Verify successful exit
                            assert exc_info.value.code == 0
                            
                            # Verify logging occurred
                            log_calls = [str(call) for call in mock_logger.info.call_args_list]
                            
                            # Check for streaming orchestrator log message
                            streaming_logged = any(
                                'streaming batch orchestrator' in str(call).lower() 
                                for call in log_calls
                            )
                            assert streaming_logged, "Should log streaming batch orchestrator initialization"
                            
                            # Check for batch size and write mode in logs
                            config_logged = any(
                                'batch_size=500' in str(call) and 'write_mode=append' in str(call)
                                for call in log_calls
                            )
                            assert config_logged, "Should log batch_size and write_mode"
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_batch_orchestrator_logs_when_streaming_disabled(self):
        """Test that batch orchestrator selection is logged when streaming disabled."""
        # Create test configuration with streaming batch disabled
        config_data = {
            'input_s3_bucket': 's3://test-input-bucket/',
            'output_s3_bucket': 's3://test-output-bucket/',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [
                {
                    'model_id': 'model-1',
                    'requests_per_minute': 10,
                    'tokens_per_minute': 1000
                }
            ],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock command-line arguments
            test_args = ['main.py', '--config', tmp_path]
            
            with patch('sys.argv', test_args):
                with patch('main.BatchOrchestrator') as mock_batch_class:
                    with patch('main.configure_logging'):
                        # Mock logging.getLogger to capture log calls
                        with patch('logging.getLogger') as mock_get_logger:
                            mock_logger = Mock()
                            mock_get_logger.return_value = mock_logger
                            
                            # Import and run main
                            from main import main
                            
                            # Mock orchestrator.run() to prevent actual execution
                            mock_batch_instance = Mock()
                            mock_batch_class.return_value = mock_batch_instance
                            
                            # Run main
                            with pytest.raises(SystemExit) as exc_info:
                                main()
                            
                            # Verify successful exit
                            assert exc_info.value.code == 0
                            
                            # Verify logging occurred
                            log_calls = [str(call) for call in mock_logger.info.call_args_list]
                            
                            # Check for batch orchestrator log message with disabled note
                            batch_logged = any(
                                'batch orchestrator' in str(call).lower() and 
                                'streaming batch disabled' in str(call).lower()
                                for call in log_calls
                            )
                            assert batch_logged, "Should log batch orchestrator with streaming disabled note"
        
        finally:
            import os
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
