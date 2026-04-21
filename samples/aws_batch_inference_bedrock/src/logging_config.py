"""Logging configuration module for structured logging."""

import logging
import sys
from typing import Optional


class ContextFilter(logging.Filter):
    """Filter to add context information to log records.
    
    This filter allows adding contextual information like article_id, symbol,
    component, and model_id to log records for better traceability.
    """
    
    def filter(self, record):
        """Add default values for context fields if not present."""
        if not hasattr(record, 'article_id'):
            record.article_id = ''
        if not hasattr(record, 'symbol'):
            record.symbol = ''
        if not hasattr(record, 'component'):
            record.component = ''
        if not hasattr(record, 'model_id'):
            record.model_id = ''
        return True


def configure_logging(level: str = 'INFO') -> None:
    """Configure Python logging module with structured format.
    
    Sets up logging with:
    - Structured format including timestamp, level, module, and message
    - Context fields for article_id, symbol, component, model_id
    - Console output to stderr
    - Configurable log level
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter with structured format
    # Include context fields when they are present
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    
    # Add context filter
    context_filter = ContextFilter()
    console_handler.addFilter(context_filter)
    
    root_logger.addHandler(console_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    
    root_logger.info(f"Logging configured: level={level}")


class LogContext:
    """Context manager for adding structured context to log messages.
    
    Usage:
        with LogContext(article_id='123', symbol='AAPL', component='headline'):
            logger.info("Processing article")
            # Log message will include article_id, symbol, component in context
    """
    
    def __init__(
        self,
        article_id: Optional[str] = None,
        symbol: Optional[str] = None,
        component: Optional[str] = None,
        model_id: Optional[str] = None
    ):
        """Initialize log context.
        
        Args:
            article_id: Article identifier
            symbol: Stock symbol
            component: Component type (content, headline, summary)
            model_id: Model identifier
        """
        self.context = {}
        if article_id:
            self.context['article_id'] = article_id
        if symbol:
            self.context['symbol'] = symbol
        if component:
            self.context['component'] = component
        if model_id:
            self.context['model_id'] = model_id
        
        self.old_factory = None
    
    def __enter__(self):
        """Enter context and add fields to log records."""
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record
        
        self.old_factory = old_factory
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore original log record factory."""
        if self.old_factory:
            logging.setLogRecordFactory(self.old_factory)
