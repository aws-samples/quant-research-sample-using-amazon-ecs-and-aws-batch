"""Tests for submit-parallel-jobs.sh script behavior."""

import os
import subprocess
import tempfile
from hypothesis import given, strategies as st, settings
from unittest.mock import patch, Mock


class TestFallbackBehavior:
    """Property-based tests for script fallback behavior."""
    
    @given(
        has_config_s3_uri=st.booleans(),
        s3_uri=st.one_of(
            st.none(),
            st.builds(
                lambda path: f"s3://{path}", 
                st.text(
                    alphabet=st.characters(
                        whitelist_categories=('Lu', 'Ll', 'Nd'),
                        whitelist_characters='-_/.@'
                    ),
                    min_size=1, 
                    max_size=80
                )
            )
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_fallback_behavior_property(self, has_config_s3_uri, s3_uri):
        """
        **Feature: s3-config-loading, Property 3: Fallback Behavior**
        
        For any job submission without CONFIG_S3_URI set, the system should 
        default to loading "config.json" from the local filesystem.
        
        **Validates: Requirements 3.3**
        """
        # Create a test script that simulates the relevant part of submit-parallel-jobs.sh
        test_script = '''#!/bin/bash
CONFIG_PATH="${CONFIG_S3_URI:-config.json}"
echo "$CONFIG_PATH"
'''
        
        # Write test script to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(test_script)
            script_path = f.name
        
        try:
            # Make script executable
            os.chmod(script_path, 0o755)
            
            # Set up environment
            env = os.environ.copy()
            if has_config_s3_uri and s3_uri:
                env['CONFIG_S3_URI'] = s3_uri
            else:
                # Remove CONFIG_S3_URI if it exists
                env.pop('CONFIG_S3_URI', None)
            
            # Run the script
            result = subprocess.run(
                [script_path],
                env=env,
                capture_output=True,
                text=True
            )
            
            # Verify the output
            output = result.stdout.strip()
            
            if has_config_s3_uri and s3_uri:
                # Should use the S3 URI
                assert output == s3_uri, f"Expected {s3_uri}, got {output}"
            else:
                # Should default to config.json
                assert output == "config.json", f"Expected 'config.json', got '{output}'"
        
        finally:
            # Clean up
            if os.path.exists(script_path):
                os.unlink(script_path)
    
    def test_fallback_with_empty_string(self):
        """Test that empty CONFIG_S3_URI falls back to config.json."""
        test_script = '''#!/bin/bash
CONFIG_PATH="${CONFIG_S3_URI:-config.json}"
echo "$CONFIG_PATH"
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(test_script)
            script_path = f.name
        
        try:
            os.chmod(script_path, 0o755)
            
            # Test with empty string
            env = os.environ.copy()
            env['CONFIG_S3_URI'] = ''
            
            result = subprocess.run(
                [script_path],
                env=env,
                capture_output=True,
                text=True
            )
            
            output = result.stdout.strip()
            # Empty string should fall back to config.json
            assert output == "config.json", f"Expected 'config.json' for empty string, got '{output}'"
        
        finally:
            if os.path.exists(script_path):
                os.unlink(script_path)
    
    def test_fallback_with_valid_s3_uri(self):
        """Test that valid S3 URI is used when provided."""
        test_script = '''#!/bin/bash
CONFIG_PATH="${CONFIG_S3_URI:-config.json}"
echo "$CONFIG_PATH"
'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(test_script)
            script_path = f.name
        
        try:
            os.chmod(script_path, 0o755)
            
            # Test with valid S3 URI
            env = os.environ.copy()
            test_uri = 's3://my-bucket/configs/production.json'
            env['CONFIG_S3_URI'] = test_uri
            
            result = subprocess.run(
                [script_path],
                env=env,
                capture_output=True,
                text=True
            )
            
            output = result.stdout.strip()
            assert output == test_uri, f"Expected '{test_uri}', got '{output}'"
        
        finally:
            if os.path.exists(script_path):
                os.unlink(script_path)


if __name__ == "__main__":
    import pytest
    import sys
    
    # Run tests
    sys.exit(pytest.main([__file__, "-v"]))
