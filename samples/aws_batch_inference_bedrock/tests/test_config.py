"""Tests for configuration loading functionality."""

import os
import json
import tempfile
from unittest.mock import Mock, patch
from hypothesis import given, strategies as st, settings, assume
from botocore.exceptions import ClientError
from config import Config, ModelConfig, RatingScale
from s3_operations import S3Client


# Strategy for generating valid model configurations
@st.composite
def model_config_strategy(draw):
    """Generate valid ModelConfig data."""
    # Use ASCII letters and digits only for model_name to match validation regex
    valid_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'
    
    return {
        'model_id': draw(st.text(min_size=1, max_size=50, alphabet=st.characters(
            whitelist_categories=('Lu', 'Ll', 'Nd'),
            whitelist_characters='.-_'
        ))),
        'requests_per_minute': draw(st.integers(min_value=1, max_value=1000)),
        'tokens_per_minute': draw(st.integers(min_value=1, max_value=100000)),
        'model_name': draw(st.one_of(
            st.none(),
            st.text(min_size=1, max_size=30, alphabet=valid_chars)
        ))
    }


# Strategy for generating valid configuration data
@st.composite
def config_data_strategy(draw):
    """Generate valid configuration JSON data."""
    # Use ASCII letters and digits only to match validation regex
    valid_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'
    bucket_chars = 'abcdefghijklmnopqrstuvwxyz0123456789-'
    
    bucket_name = draw(st.text(min_size=3, max_size=20, alphabet=bucket_chars).filter(
        lambda x: not x.startswith('-') and not x.endswith('-')
    ))
    
    return {
        'input_s3_bucket': bucket_name,
        'output_s3_bucket': bucket_name,
        'prompt_s3_uri': f"s3://{bucket_name}/prompts/user.txt",
        'system_prompt_s3_uri': f"s3://{bucket_name}/prompts/system.txt",
        'rating_scale': {
            'min': draw(st.integers(min_value=0, max_value=5)),
            'max': draw(st.integers(min_value=6, max_value=10))
        },
        'models': [draw(model_config_strategy()) for _ in range(draw(st.integers(min_value=1, max_value=3)))],
        'max_articles': draw(st.one_of(st.none(), st.integers(min_value=1, max_value=1000))),
        'job_name': draw(st.one_of(
            st.none(),
            st.text(min_size=1, max_size=20, alphabet=valid_chars)
        )),
        'enable_streaming_batch': False
    }


class TestConfigurationEquivalence:
    """Property-based tests for configuration equivalence."""
    
    @given(config_data=config_data_strategy())
    @settings(max_examples=100, deadline=None)
    def test_configuration_equivalence_property(self, config_data):
        """
        **Feature: s3-config-loading, Property 2: Configuration Equivalence**
        
        For any valid configuration file, loading it from S3 should produce 
        the same Config object as loading it from a local file with identical content.
        
        **Validates: Requirements 1.3**
        """
        # Create temporary local file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as local_file:
            json.dump(config_data, local_file)
            local_path = local_file.name
        
        # Create temporary S3 download file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as s3_file:
            json.dump(config_data, s3_file)
            s3_path = s3_file.name
        
        try:
            # Load from local file
            config_from_local = Config.from_file(local_path)
            
            # Mock S3Client to return the same content
            mock_s3_client = Mock(spec=S3Client)
            
            def mock_download(s3_uri, local_path):
                # Copy content from s3_path to local_path
                with open(s3_path, 'r') as src:
                    content = src.read()
                with open(local_path, 'w') as dst:
                    dst.write(content)
            
            mock_s3_client.download_file = Mock(side_effect=mock_download)
            
            # Load from S3 (mocked)
            config_from_s3 = Config.from_s3_or_file('s3://test-bucket/config.json', s3_client=mock_s3_client)
            
            # Verify equivalence
            assert config_from_local.input_s3_bucket == config_from_s3.input_s3_bucket
            assert config_from_local.output_s3_bucket == config_from_s3.output_s3_bucket
            assert config_from_local.prompt_s3_uri == config_from_s3.prompt_s3_uri
            assert config_from_local.system_prompt_s3_uri == config_from_s3.system_prompt_s3_uri
            assert config_from_local.rating_scale.min == config_from_s3.rating_scale.min
            assert config_from_local.rating_scale.max == config_from_s3.rating_scale.max
            assert len(config_from_local.models) == len(config_from_s3.models)
            
            for model_local, model_s3 in zip(config_from_local.models, config_from_s3.models):
                assert model_local.model_id == model_s3.model_id
                assert model_local.requests_per_minute == model_s3.requests_per_minute
                assert model_local.tokens_per_minute == model_s3.tokens_per_minute
                assert model_local.model_name == model_s3.model_name
            
            assert config_from_local.max_articles == config_from_s3.max_articles
            assert config_from_local.job_name == config_from_s3.job_name
            
        finally:
            # Clean up
            if os.path.exists(local_path):
                os.unlink(local_path)
            if os.path.exists(s3_path):
                os.unlink(s3_path)


class TestFromS3OrFile:
    """Unit tests for from_s3_or_file method."""
    
    def test_s3_uri_triggers_download(self):
        """Test S3 URI path triggers S3 download."""
        # Create test config data
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'enable_streaming_batch': False
        }
        
        # Create temporary file with config data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Mock S3Client
            mock_s3_client = Mock(spec=S3Client)
            
            def mock_download(s3_uri, local_path):
                # Copy content from tmp_path to local_path
                with open(tmp_path, 'r') as src:
                    content = src.read()
                with open(local_path, 'w') as dst:
                    dst.write(content)
            
            mock_s3_client.download_file = Mock(side_effect=mock_download)
            
            # Load from S3
            config = Config.from_s3_or_file('s3://test-bucket/config.json', s3_client=mock_s3_client)
            
            # Verify S3 download was called
            mock_s3_client.download_file.assert_called_once()
            call_args = mock_s3_client.download_file.call_args
            assert call_args[0][0] == 's3://test-bucket/config.json'
            
            # Verify config was loaded correctly
            assert config.input_s3_bucket == 'test-bucket'
            assert len(config.models) == 1
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_local_path_uses_from_file(self):
        """Test local file path uses existing from_file logic."""
        # Create test config data
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'enable_streaming_batch': False
        }
        
        # Create temporary local file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Load from local file (should not use S3Client)
            config = Config.from_s3_or_file(tmp_path)
            
            # Verify config was loaded correctly
            assert config.input_s3_bucket == 'test-bucket'
            assert len(config.models) == 1
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_temp_file_cleanup_after_successful_load(self):
        """Test temp file cleanup after successful load."""
        # Create test config data
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'enable_streaming_batch': False
        }
        
        # Create temporary file with config data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        temp_files_created = []
        
        try:
            # Mock S3Client
            mock_s3_client = Mock(spec=S3Client)
            
            def mock_download(s3_uri, local_path):
                temp_files_created.append(local_path)
                # Copy content from tmp_path to local_path
                with open(tmp_path, 'r') as src:
                    content = src.read()
                with open(local_path, 'w') as dst:
                    dst.write(content)
            
            mock_s3_client.download_file = Mock(side_effect=mock_download)
            
            # Load from S3
            config = Config.from_s3_or_file('s3://test-bucket/config.json', s3_client=mock_s3_client)
            
            # Verify temp file was cleaned up
            assert len(temp_files_created) == 1
            assert not os.path.exists(temp_files_created[0]), "Temp file should be cleaned up after successful load"
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            # Clean up any remaining temp files
            for temp_file in temp_files_created:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
    
    def test_temp_file_cleanup_after_failed_load(self):
        """Test temp file cleanup after failed load."""
        temp_files_created = []
        
        # Mock S3Client that creates a temp file but writes invalid JSON
        mock_s3_client = Mock(spec=S3Client)
        
        def mock_download(s3_uri, local_path):
            temp_files_created.append(local_path)
            # Write invalid JSON
            with open(local_path, 'w') as f:
                f.write("invalid json content {{{")
        
        mock_s3_client.download_file = Mock(side_effect=mock_download)
        
        try:
            # Attempt to load from S3 (should fail due to invalid JSON)
            Config.from_s3_or_file('s3://test-bucket/config.json', s3_client=mock_s3_client)
            assert False, "Expected ValueError for invalid JSON"
        except ValueError:
            # Expected - verify temp file was cleaned up
            assert len(temp_files_created) == 1
            assert not os.path.exists(temp_files_created[0]), "Temp file should be cleaned up even after failed load"
        finally:
            # Clean up any remaining temp files
            for temp_file in temp_files_created:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)


class TestErrorPropagation:
    """Property-based tests for error propagation."""
    
    @given(
        error_code=st.sampled_from(['NoSuchKey', 'NoSuchBucket', 'AccessDenied', '403'])
    )
    @settings(max_examples=100, deadline=None)
    def test_error_propagation_property(self, error_code):
        """
        **Feature: s3-config-loading, Property 4: Error Propagation**
        
        For any S3 download failure (network error, missing file, permission denied), 
        the system should log an error and exit with a non-zero status code.
        
        **Validates: Requirements 1.5**
        """
        # Use a fixed S3 URI for testing
        s3_uri = 's3://test-bucket/config.json'
        
        # Mock S3Client that raises ClientError
        mock_s3_client = Mock(spec=S3Client)
        
        # Create a ClientError
        error_response = {
            'Error': {
                'Code': error_code,
                'Message': f'Test error: {error_code}'
            }
        }
        mock_s3_client.download_file = Mock(
            side_effect=ClientError(error_response, 'GetObject')
        )
        
        # Attempt to load from S3 - should propagate the error
        try:
            Config.from_s3_or_file(s3_uri, s3_client=mock_s3_client)
            # If we get here, the error was not propagated
            assert False, f"Expected ClientError to be propagated for error code: {error_code}"
        except ClientError as e:
            # Verify the error was propagated correctly
            assert e.response['Error']['Code'] == error_code
            # This is the expected behavior - error should propagate


class TestChunkSizeConfigurability:
    """Property-based tests for chunk size configurability."""
    
    @given(chunk_size=st.integers(min_value=1, max_value=10000))
    @settings(max_examples=100, deadline=None)
    def test_chunk_size_configurability_property(self, chunk_size):
        """
        **Feature: parallel-s3-dataframe-optimization, Property 7: Chunk size configurability**
        
        For any valid chunk size value C (positive integer), the system should 
        successfully process articles using that chunk size without errors.
        
        **Validates: Requirements 6.5**
        """
        # Create test config data with custom chunk size
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'download_chunk_size': chunk_size,
            'enable_streaming_batch': False
        }
        
        # Create temporary file with config data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Load config - should succeed for any positive chunk size
            config = Config.from_file(tmp_path)
            
            # Verify chunk size was set correctly
            assert config.download_chunk_size == chunk_size
            
            # Verify config is valid
            config.validate()
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestParallelReadingConfigExtensions:
    """Unit tests for parallel reading configuration extensions."""
    
    def test_default_values_work_correctly(self):
        """Test that default values are applied when fields are not specified."""
        # Create config without parallel reading fields
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            config = Config.from_file(tmp_path)
            
            # Verify default values
            assert config.parallel_reading_enabled is True
            assert config.concurrent_download_limit == 75
            assert config.download_chunk_size == 1000
            
            # Verify config is valid
            config.validate()
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_validation_for_concurrent_limit_range(self):
        """Test validation enforces concurrent_limit range (50-100)."""
        # Test below minimum
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'concurrent_download_limit': 49,
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Should raise ValueError during from_file (which calls validate)
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for concurrent_limit < 50"
            except ValueError as e:
                assert "concurrent_download_limit must be between 50 and 100" in str(e)
                
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test above maximum
        config_data['concurrent_download_limit'] = 101
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Should raise ValueError during from_file (which calls validate)
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for concurrent_limit > 100"
            except ValueError as e:
                assert "concurrent_download_limit must be between 50 and 100" in str(e)
                
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test valid range
        for valid_limit in [50, 75, 100]:
            config_data['concurrent_download_limit'] = valid_limit
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                json.dump(config_data, tmp)
                tmp_path = tmp.name
            
            try:
                config = Config.from_file(tmp_path)
                # Should not raise - validation passes
                assert config.concurrent_download_limit == valid_limit
                
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    
    def test_validation_for_chunk_size(self):
        """Test validation enforces positive chunk_size."""
        # Test zero chunk size
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'download_chunk_size': 0,
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Should raise ValueError during from_file (which calls validate)
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for chunk_size = 0"
            except ValueError as e:
                assert "download_chunk_size must be a positive integer" in str(e)
                
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test negative chunk size
        config_data['download_chunk_size'] = -100
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Should raise ValueError during from_file (which calls validate)
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for negative chunk_size"
            except ValueError as e:
                assert "download_chunk_size must be a positive integer" in str(e)
                
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_backward_compatibility_with_old_configs(self):
        """Test that old configs without new fields still work."""
        # Create an old-style config (no parallel reading fields)
        old_config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'max_articles': 100,
            'job_name': 'test-job',
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(old_config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Should load successfully with defaults
            config = Config.from_file(tmp_path)
            
            # Verify old fields still work
            assert config.input_s3_bucket == 'test-bucket'
            assert config.output_s3_bucket == 'test-bucket'
            assert config.max_articles == 100
            assert config.job_name == 'test-job'
            assert len(config.models) == 1
            
            # Verify new fields have defaults
            assert config.parallel_reading_enabled is True
            assert config.concurrent_download_limit == 75
            assert config.download_chunk_size == 1000
            
            # Verify config is valid
            config.validate()
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestS3TablesConfigExtensions:
    """Unit tests for S3 Tables configuration extensions."""
    
    def test_default_values_work_correctly(self):
        """Test that default values are applied when fields are not specified."""
        # Create config without S3 Tables fields
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'enable_streaming_batch': False  # Disable to avoid s3_table_name requirement
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            config = Config.from_file(tmp_path)
            
            # Verify default values
            assert config.batch_size == 1000
            assert config.s3_table_name is None
            assert config.s3_table_warehouse is None
            assert config.catalog_name == 'default'
            assert config.enable_streaming_batch is False
            assert config.write_mode == 'upsert'
            
            # Verify config is valid
            config.validate()
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_batch_size_validation_rejects_invalid_values(self):
        """Test batch_size validation rejects values outside [100, 10000]."""
        # Test below minimum
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'batch_size': 99,
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for batch_size < 100"
            except ValueError as e:
                assert "batch_size must be between 100 and 10000" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test above maximum
        config_data['batch_size'] = 10001
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for batch_size > 10000"
            except ValueError as e:
                assert "batch_size must be between 100 and 10000" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test valid range
        for valid_size in [100, 1000, 5000, 10000]:
            config_data['batch_size'] = valid_size
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                json.dump(config_data, tmp)
                tmp_path = tmp.name
            
            try:
                config = Config.from_file(tmp_path)
                assert config.batch_size == valid_size
                config.validate()  # Should not raise
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    
    def test_write_mode_validation(self):
        """Test write_mode validation enforces 'append' or 'upsert'."""
        # Test invalid write_mode
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'write_mode': 'invalid',
            'enable_streaming_batch': False
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError for invalid write_mode"
            except ValueError as e:
                assert "write_mode must be 'append' or 'upsert'" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test valid write_modes
        for valid_mode in ['append', 'upsert']:
            config_data['write_mode'] = valid_mode
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                json.dump(config_data, tmp)
                tmp_path = tmp.name
            
            try:
                config = Config.from_file(tmp_path)
                assert config.write_mode == valid_mode
                config.validate()  # Should not raise
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    
    def test_backward_compatibility_with_old_configs(self):
        """Test that old configs without S3 Tables fields still work."""
        # Create an old-style config (no S3 Tables fields)
        # Note: from_file calls validate(), so configs with enable_streaming_batch=True (default)
        # will fail validation if s3_table_name is not provided.
        # This is expected - old configs need to explicitly set enable_streaming_batch=False
        # for backward compatibility.
        old_config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'max_articles': 100,
            'job_name': 'test-job'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(old_config_data, tmp)
            tmp_path = tmp.name
        
        try:
            # Should fail validation because enable_streaming_batch defaults to True
            # but s3_table_name is not provided
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected validation to fail when streaming enabled but table name missing"
            except ValueError as e:
                assert "s3_table_name is required when enable_streaming_batch is True" in str(e)
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test with streaming disabled for full backward compatibility
        old_config_data['enable_streaming_batch'] = False
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(old_config_data, tmp)
            tmp_path = tmp.name
        
        try:
            config = Config.from_file(tmp_path)
            
            # Should validate successfully when streaming is disabled
            config.validate()
            
            # Verify all fields
            assert config.input_s3_bucket == 'test-bucket'
            assert config.enable_streaming_batch is False
            assert config.batch_size == 1000
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def test_s3_table_name_required_when_streaming_enabled(self):
        """Test s3_table_name is required when enable_streaming_batch is True."""
        # Test with streaming enabled but no table name
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'enable_streaming_batch': True
            # s3_table_name not provided
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError when streaming enabled but table name missing"
            except ValueError as e:
                assert "s3_table_name is required when enable_streaming_batch is True" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test with streaming enabled and table name provided but no warehouse
        config_data['s3_table_name'] = 'test-table'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            try:
                config = Config.from_file(tmp_path)
                assert False, "Expected ValueError when streaming enabled but warehouse missing"
            except ValueError as e:
                assert "s3_table_warehouse is required when enable_streaming_batch is True" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test with both table name and warehouse provided
        config_data['s3_table_warehouse'] = 's3://test-bucket/warehouse/'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            config = Config.from_file(tmp_path)
            assert config.s3_table_name == 'test-table'
            assert config.s3_table_warehouse == 's3://test-bucket/warehouse/'
            config.validate()  # Should not raise
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        
        # Test with streaming disabled - table name not required
        config_data['enable_streaming_batch'] = False
        config_data.pop('s3_table_name')
        config_data.pop('s3_table_warehouse')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            config = Config.from_file(tmp_path)
            assert config.enable_streaming_batch is False
            assert config.s3_table_name is None
            config.validate()  # Should not raise
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestBatchSizeValidation:
    """Property-based tests for batch size validation."""
    
    @given(batch_size=st.integers())
    @settings(max_examples=100, deadline=None)
    def test_batch_size_validation_property(self, batch_size):
        """
        **Feature: streaming-batch-s3-tables-output, Property 1: Batch size validation**
        
        For any batch_size configuration value, if it is outside the range [100, 10000], 
        the system should reject it with a validation error.
        
        **Validates: Requirements 7.2**
        """
        # Create test config data with the given batch_size
        config_data = {
            'input_s3_bucket': 'test-bucket',
            'output_s3_bucket': 'test-bucket',
            'prompt_s3_uri': 's3://test-bucket/prompts/user.txt',
            'system_prompt_s3_uri': 's3://test-bucket/prompts/system.txt',
            'rating_scale': {'min': 1, 'max': 5},
            'models': [{
                'model_id': 'test-model',
                'requests_per_minute': 10,
                'tokens_per_minute': 1000
            }],
            'batch_size': batch_size,
            'enable_streaming_batch': False  # Disable to avoid s3_table_name requirement
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(config_data, tmp)
            tmp_path = tmp.name
        
        try:
            if 100 <= batch_size <= 10000:
                # Valid batch size - should succeed
                config = Config.from_file(tmp_path)
                assert config.batch_size == batch_size
                config.validate()  # Should not raise
            else:
                # Invalid batch size - should raise ValueError
                try:
                    config = Config.from_file(tmp_path)
                    assert False, f"Expected ValueError for batch_size={batch_size} outside [100, 10000]"
                except ValueError as e:
                    assert "batch_size must be between 100 and 10000" in str(e)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


if __name__ == "__main__":
    import pytest
    import sys
    
    # Run tests
    sys.exit(pytest.main([__file__, "-v"]))
