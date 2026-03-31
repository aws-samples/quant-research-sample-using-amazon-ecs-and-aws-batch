#!/usr/bin/env python3
"""CLI entry point for AWS Bedrock News Sentiment Analysis System."""

import argparse
import logging
import sys
from pathlib import Path

from botocore.exceptions import ClientError
from config import Config
from logging_config import configure_logging
from orchestrator import BatchOrchestrator, StreamingBatchOrchestrator


def parse_arguments():
    """Parse command-line arguments.
    
    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='AWS Bedrock News Sentiment Analysis System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python main.py --config config.json
  python main.py --config /path/to/config.json --log-level DEBUG
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to JSON configuration file'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--model-id',
        type=str,
        default=None,
        help='Process only the specified model ID (if not provided, processes all models)'
    )
    
    return parser.parse_args()


def main():
    """Main execution flow for sentiment analysis batch processing.
    
    Implements:
    - Configuration loading and validation (Requirement 1.1)
    - Batch orchestrator execution (Requirement 6.1)
    - Error handling with appropriate exit codes (Requirement 6.3, 6.4, 6.5)
    """
    # Parse command-line arguments
    args = parse_arguments()
    
    # Setup logging
    configure_logging(level=args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("AWS Bedrock News Sentiment Analysis System")
    logger.info("=" * 80)
    logger.info(f"Configuration file: {args.config}")
    logger.info(f"Log level: {args.log_level}")
    
    try:
        # Load and validate configuration (Requirement 1.1)
        logger.info("Loading configuration...")
        config_path_str = args.config
        
        # Detect if this is an S3 URI or local file
        if config_path_str.startswith('s3://'):
            logger.info(f"Using S3 configuration: {config_path_str}")
        else:
            logger.info(f"Using local configuration: {config_path_str}")
            config_path = Path(config_path_str)
            if not config_path.exists():
                logger.error(f"Configuration file not found: {config_path_str}")
                sys.exit(1)
        
        try:
            config = Config.from_s3_or_file(config_path_str)
            logger.info("Configuration loaded successfully")
            
            # Filter models if --model-id is specified
            if args.model_id:
                logger.info(f"Filtering configuration for model: {args.model_id}")
                original_count = len(config.models)
                original_models = [m.model_id for m in config.models]
                config.models = [m for m in config.models if m.model_id == args.model_id]
                
                if not config.models:
                    logger.error(
                        f"Model ID '{args.model_id}' not found in configuration. "
                        f"Available models: {original_models}"
                    )
                    sys.exit(1)
                
                logger.info(
                    f"Filtered to 1 model (from {original_count} total models)"
                )
            
        except ValueError as e:
            logger.error(f"Invalid configuration: {e}")
            sys.exit(1)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"S3 error while loading configuration: {error_code} - {error_message}")
            logger.error(f"Failed to download configuration from S3: {config_path_str}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}", exc_info=True)
            sys.exit(1)
        
        # Validate configuration
        logger.info("Validating configuration...")
        try:
            config.validate()
            logger.info("Configuration validated successfully")
        except ValueError as e:
            logger.error(f"Configuration validation failed: {e}")
            sys.exit(1)
        
        # Log configuration summary
        logger.info(f"Input S3 bucket: {config.input_s3_bucket}")
        logger.info(f"Output S3 bucket: {config.output_s3_bucket}")
        logger.info(f"Prompt S3 URI: {config.prompt_s3_uri}")
        logger.info(
            f"Rating scale: [{config.rating_scale.min}, {config.rating_scale.max}]"
        )
        logger.info(f"Max articles: {config.max_articles if config.max_articles else 'unlimited'}")
        logger.info(f"Job name: {config.job_name if config.job_name else 'none'}")
        logger.info(f"Models configured: {len(config.models)}")
        for i, model in enumerate(config.models, 1):
            logger.info(
                f"  Model {i}: {model.model_id} "
                f"({model.requests_per_minute} req/min, "
                f"{model.tokens_per_minute} tokens/min)"
            )
        
        # Initialize and run orchestrator (Requirement 6.1, 8.1)
        # Select orchestrator based on enable_streaming_batch config flag
        if config.enable_streaming_batch:
            logger.info("Initializing streaming batch orchestrator...")
            logger.info(
                f"Streaming batch mode enabled: batch_size={config.batch_size}, "
                f"write_mode={config.write_mode}, "
                f"s3_table_name={config.s3_table_name}"
            )
            orchestrator = StreamingBatchOrchestrator(config)
        else:
            logger.info("Initializing batch orchestrator (streaming batch disabled)...")
            orchestrator = BatchOrchestrator(config)
        
        logger.info("Starting batch processing...")
        orchestrator.run()
        
        logger.info("=" * 80)
        logger.info("Batch processing completed successfully")
        logger.info("=" * 80)
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.warning("Batch processing interrupted by user")
        sys.exit(130)  # Standard exit code for SIGINT
        
    except Exception as e:
        logger.error(
            f"Fatal error during batch processing: {e}",
            exc_info=True
        )
        logger.error("=" * 80)
        logger.error("Batch processing failed")
        logger.error("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
