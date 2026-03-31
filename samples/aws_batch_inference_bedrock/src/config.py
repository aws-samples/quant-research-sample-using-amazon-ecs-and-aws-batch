"""Configuration management module for sentiment analysis system."""

import json
import logging
from dataclasses import dataclass
from typing import List, Optional


logger = logging.getLogger(__name__)


@dataclass
class RatingScale:
    """Rating scale configuration for sentiment scores."""
    min: int
    max: int
    
    def validate(self) -> None:
        """Validate rating scale parameters."""
        if self.min >= self.max:
            raise ValueError(f"Rating scale min ({self.min}) must be less than max ({self.max})")
        if self.max - self.min < 1:
            raise ValueError(f"Rating scale range must be at least 1")


@dataclass
class ModelConfig:
    """Configuration for a single Bedrock model.
    
    Either model_id or model_arn must be provided (but not both).
    - model_id: Standard Bedrock model identifier (e.g., "us.anthropic.claude-3-haiku-20240307-v1:0")
    - model_arn: Custom model ARN for provisioned throughput or imported models
    """
    requests_per_minute: int
    tokens_per_minute: int
    model_id: Optional[str] = None
    model_arn: Optional[str] = None
    model_name: Optional[str] = None  # Optional friendly name for output files
    
    def get_model_identifier(self) -> str:
        """Get the model identifier (either model_id or model_arn).
        
        Returns:
            The model_id if provided, otherwise model_arn
        """
        return self.model_id if self.model_id else self.model_arn
    
    def validate(self) -> None:
        """Validate model configuration parameters."""
        # Exactly one of model_id or model_arn must be provided
        if not self.model_id and not self.model_arn:
            raise ValueError("Either model_id or model_arn must be provided")
        if self.model_id and self.model_arn:
            raise ValueError("Cannot specify both model_id and model_arn, choose one")
        
        # Validate the provided identifier is not empty
        identifier = self.get_model_identifier()
        if not identifier or not identifier.strip():
            raise ValueError("model_id or model_arn cannot be empty")
        
        if self.requests_per_minute <= 0:
            raise ValueError(f"requests_per_minute must be positive, got {self.requests_per_minute}")
        if self.tokens_per_minute <= 0:
            raise ValueError(f"tokens_per_minute must be positive, got {self.tokens_per_minute}")
        
        # Validate model_name if provided
        if self.model_name is not None:
            if not isinstance(self.model_name, str) or not self.model_name.strip():
                raise ValueError(f"model_name must be a non-empty string, got: {self.model_name}")
            # Check for invalid characters in model_name (for safe filenames)
            import re
            if not re.match(r'^[a-zA-Z0-9_-]+$', self.model_name):
                raise ValueError(f"model_name can only contain letters, numbers, hyphens, and underscores, got: {self.model_name}")


@dataclass
class Config:
    """Main configuration container for the sentiment analysis system."""
    input_s3_bucket: str
    output_s3_bucket: str
    prompt_s3_uri: str
    system_prompt_s3_uri: str
    rating_scale: RatingScale
    models: List[ModelConfig]
    max_articles: Optional[int] = None  # None means unlimited
    job_name: Optional[str] = None  # Optional identifier for the job run
    parallel_reading_enabled: bool = True  # Enable parallel S3 reading
    concurrent_download_limit: int = 75  # Max concurrent S3 downloads (50-100)
    download_chunk_size: int = 1000  # Files per batch chunk
    # S3 Tables streaming batch configuration
    batch_size: int = 1000  # Number of articles per streaming batch (100-10000)
    s3_table_name: Optional[str] = None  # Name of S3 Table for results
    s3_table_warehouse: Optional[str] = None  # S3 path to table warehouse
    catalog_name: str = "default"  # Iceberg catalog name
    enable_streaming_batch: bool = True  # Enable streaming batch processing
    write_mode: str = "upsert"  # Write mode: "append" or "upsert"
    
    @classmethod
    def from_s3_or_file(cls, config_path: str, s3_client=None) -> 'Config':
        """Load configuration from S3 URI or local file.
        
        Args:
            config_path: Either an S3 URI (s3://...) or local file path
            s3_client: Optional S3Client instance for testing
            
        Returns:
            Config object with loaded parameters
            
        Raises:
            ValueError: If config_path is invalid or config is malformed
            ClientError: If S3 download fails
        """
        import tempfile
        import os
        from s3_operations import S3Client
        
        # Detect if this is an S3 URI
        if config_path.startswith('s3://'):
            logger.info(f"Detected S3 URI for configuration: {config_path}")
            
            # Create S3Client if not provided
            if s3_client is None:
                s3_client = S3Client()
            
            # Download to temporary file
            temp_file = None
            try:
                # Create temporary file
                fd, temp_file = tempfile.mkstemp(suffix='.json', prefix='config_')
                os.close(fd)  # Close the file descriptor, we'll write via S3Client
                
                logger.info(f"Downloading S3 config to temporary file: {temp_file}")
                s3_client.download_file(config_path, temp_file)
                
                # Load config from temp file
                config = cls.from_file(temp_file)
                logger.info("Successfully loaded configuration from S3")
                return config
                
            finally:
                # Clean up temp file
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.unlink(temp_file)
                        logger.debug(f"Cleaned up temporary config file: {temp_file}")
                    except Exception as e:
                        logger.warning(f"Failed to clean up temporary file {temp_file}: {e}")
        else:
            # Local file path - use existing from_file method
            logger.info(f"Loading configuration from local file: {config_path}")
            return cls.from_file(config_path)
    
    @classmethod
    def from_file(cls, filepath: str) -> 'Config':
        """Load and parse configuration from JSON file.
        
        Args:
            filepath: Path to JSON configuration file
            
        Returns:
            Config object with loaded parameters
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid JSON or missing required fields
        """
        logger.info(f"Loading configuration from {filepath}")
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.debug(f"Configuration file loaded successfully: {filepath}")
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {filepath}")
            raise FileNotFoundError(f"Configuration file not found: {filepath}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file {filepath}: {e}")
            raise ValueError(f"Invalid JSON in configuration file: {e}")
        
        # Parse rating scale
        rating_scale_data = data.get('rating_scale', {})
        rating_scale = RatingScale(
            min=rating_scale_data.get('min'),
            max=rating_scale_data.get('max')
        )
        
        # Parse models
        models_data = data.get('models', [])
        models = [
            ModelConfig(
                model_id=m.get('model_id'),
                model_arn=m.get('model_arn'),
                requests_per_minute=m.get('requests_per_minute'),
                tokens_per_minute=m.get('tokens_per_minute'),
                model_name=m.get('model_name')
            )
            for m in models_data
        ]
        
        config = cls(
            input_s3_bucket=data.get('input_s3_bucket'),
            output_s3_bucket=data.get('output_s3_bucket'),
            prompt_s3_uri=data.get('prompt_s3_uri'),
            system_prompt_s3_uri=data.get('system_prompt_s3_uri'),
            rating_scale=rating_scale,
            models=models,
            max_articles=data.get('max_articles'),
            job_name=data.get('job_name'),
            parallel_reading_enabled=data.get('parallel_reading_enabled', True),
            concurrent_download_limit=data.get('concurrent_download_limit', 75),
            download_chunk_size=data.get('download_chunk_size', 1000),
            batch_size=data.get('batch_size', 1000),
            s3_table_name=data.get('s3_table_name'),
            s3_table_warehouse=data.get('s3_table_warehouse'),
            catalog_name=data.get('catalog_name', 'default'),
            enable_streaming_batch=data.get('enable_streaming_batch', True),
            write_mode=data.get('write_mode', 'upsert')
        )
        
        logger.info(
            f"Configuration parsed: {len(models)} model(s), "
            f"rating_scale=[{rating_scale.min}, {rating_scale.max}]"
        )
        
        config.validate()
        logger.info("Configuration validation successful")
        
        return config
    
    def validate(self) -> None:
        """Validate all configuration parameters.
        
        Raises:
            ValueError: If any required field is missing or invalid
        """
        logger.debug("Validating configuration parameters")
        
        # Check required fields
        if not self.input_s3_bucket:
            logger.error("Validation failed: input_s3_bucket is required")
            raise ValueError("input_s3_bucket is required")
        if not self.output_s3_bucket:
            logger.error("Validation failed: output_s3_bucket is required")
            raise ValueError("output_s3_bucket is required")
        if not self.prompt_s3_uri:
            logger.error("Validation failed: prompt_s3_uri is required")
            raise ValueError("prompt_s3_uri is required")
        if not self.system_prompt_s3_uri:
            logger.error("Validation failed: system_prompt_s3_uri is required")
            raise ValueError("system_prompt_s3_uri is required")
        
        # Validate S3 URIs format
        if not self.prompt_s3_uri.startswith('s3://'):
            logger.error(f"Validation failed: invalid prompt_s3_uri format: {self.prompt_s3_uri}")
            raise ValueError(f"prompt_s3_uri must start with 's3://', got: {self.prompt_s3_uri}")
        if not self.system_prompt_s3_uri.startswith('s3://'):
            logger.error(f"Validation failed: invalid system_prompt_s3_uri format: {self.system_prompt_s3_uri}")
            raise ValueError(f"system_prompt_s3_uri must start with 's3://', got: {self.system_prompt_s3_uri}")
        
        # Validate rating scale
        if self.rating_scale is None:
            logger.error("Validation failed: rating_scale is required")
            raise ValueError("rating_scale is required")
        
        try:
            self.rating_scale.validate()
        except ValueError as e:
            logger.error(f"Validation failed: invalid rating_scale: {e}")
            raise
        
        # Validate models
        if not self.models:
            logger.error("Validation failed: at least one model configuration is required")
            raise ValueError("At least one model configuration is required")
        
        for i, model in enumerate(self.models):
            try:
                model.validate()
            except ValueError as e:
                logger.error(f"Validation failed: invalid model at index {i}: {e}")
                raise ValueError(f"Invalid model configuration at index {i}: {e}")
        
        # Validate max_articles if provided
        if self.max_articles is not None:
            if not isinstance(self.max_articles, int) or self.max_articles <= 0:
                logger.error(f"Validation failed: max_articles must be a positive integer, got: {self.max_articles}")
                raise ValueError(f"max_articles must be a positive integer, got: {self.max_articles}")
        
        # Validate job_name if provided
        if self.job_name is not None:
            if not isinstance(self.job_name, str) or not self.job_name.strip():
                logger.error(f"Validation failed: job_name must be a non-empty string, got: {self.job_name}")
                raise ValueError(f"job_name must be a non-empty string, got: {self.job_name}")
            # Check for invalid characters in job_name (for safe filenames)
            import re
            if not re.match(r'^[a-zA-Z0-9_-]+$', self.job_name):
                logger.error(f"Validation failed: job_name can only contain letters, numbers, hyphens, and underscores, got: {self.job_name}")
                raise ValueError(f"job_name can only contain letters, numbers, hyphens, and underscores")
        
        # Validate parallel reading settings
        if not isinstance(self.parallel_reading_enabled, bool):
            logger.error(f"Validation failed: parallel_reading_enabled must be a boolean, got: {self.parallel_reading_enabled}")
            raise ValueError(f"parallel_reading_enabled must be a boolean, got: {self.parallel_reading_enabled}")
        
        if not isinstance(self.concurrent_download_limit, int):
            logger.error(f"Validation failed: concurrent_download_limit must be an integer, got: {self.concurrent_download_limit}")
            raise ValueError(f"concurrent_download_limit must be an integer, got: {self.concurrent_download_limit}")
        
        if self.concurrent_download_limit < 50 or self.concurrent_download_limit > 100:
            logger.error(f"Validation failed: concurrent_download_limit must be between 50 and 100, got: {self.concurrent_download_limit}")
            raise ValueError(f"concurrent_download_limit must be between 50 and 100, got: {self.concurrent_download_limit}")
        
        if not isinstance(self.download_chunk_size, int):
            logger.error(f"Validation failed: download_chunk_size must be an integer, got: {self.download_chunk_size}")
            raise ValueError(f"download_chunk_size must be an integer, got: {self.download_chunk_size}")
        
        if self.download_chunk_size <= 0:
            logger.error(f"Validation failed: download_chunk_size must be a positive integer, got: {self.download_chunk_size}")
            raise ValueError(f"download_chunk_size must be a positive integer, got: {self.download_chunk_size}")
        
        # Validate S3 Tables streaming batch settings
        if not isinstance(self.batch_size, int):
            logger.error(f"Validation failed: batch_size must be an integer, got: {self.batch_size}")
            raise ValueError(f"batch_size must be an integer, got: {self.batch_size}")
        
        if self.batch_size < 100 or self.batch_size > 10000:
            logger.error(f"Validation failed: batch_size must be between 100 and 10000, got: {self.batch_size}")
            raise ValueError(f"batch_size must be between 100 and 10000, got: {self.batch_size}")
        
        if self.write_mode not in ["append", "upsert"]:
            logger.error(f"Validation failed: write_mode must be 'append' or 'upsert', got: {self.write_mode}")
            raise ValueError(f"write_mode must be 'append' or 'upsert', got: {self.write_mode}")
        
        if not isinstance(self.enable_streaming_batch, bool):
            logger.error(f"Validation failed: enable_streaming_batch must be a boolean, got: {self.enable_streaming_batch}")
            raise ValueError(f"enable_streaming_batch must be a boolean, got: {self.enable_streaming_batch}")
        
        if not isinstance(self.catalog_name, str) or not self.catalog_name.strip():
            logger.error(f"Validation failed: catalog_name must be a non-empty string, got: {self.catalog_name}")
            raise ValueError(f"catalog_name must be a non-empty string, got: {self.catalog_name}")
        
        # Validate s3_table_name is provided when streaming is enabled
        if self.enable_streaming_batch:
            if self.s3_table_name is None or not self.s3_table_name.strip():
                logger.error("Validation failed: s3_table_name is required when enable_streaming_batch is True")
                raise ValueError("s3_table_name is required when enable_streaming_batch is True")
            if self.s3_table_warehouse is None or not self.s3_table_warehouse.strip():
                logger.error("Validation failed: s3_table_warehouse is required when enable_streaming_batch is True")
                raise ValueError("s3_table_warehouse is required when enable_streaming_batch is True")
        
        logger.debug("Configuration validation complete")
# Build Tue Feb  3 14:02:07 CST 2026
