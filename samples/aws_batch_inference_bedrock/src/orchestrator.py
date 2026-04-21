"""Batch orchestrator module for end-to-end sentiment analysis workflow."""

import asyncio
import logging
import time
from typing import List, Dict
import pandas as pd
import boto3
from botocore.exceptions import ClientError

from config import Config, ModelConfig
from s3_operations import S3Client
from article_processor import ArticleProcessor, AnalysisTask
from rate_limiter import RateLimiter
from bedrock_client import BedrockClient
from sentiment_analyzer import SentimentAnalyzer, SentimentResult
from s3_tables_writer import S3TablesWriter


logger = logging.getLogger(__name__)


async def analyze_tasks_concurrently(
    tasks: List[AnalysisTask],
    sentiment_analyzer: SentimentAnalyzer,
    model_id: str,
    model_name: str,
    log_progress: bool = True
) -> List[SentimentResult]:
    """Analyze tasks concurrently with optional progress logging.
    
    Shared utility function used by both BatchOrchestrator and 
    StreamingBatchOrchestrator to avoid code duplication.
    
    Args:
        tasks: List of analysis tasks to process
        sentiment_analyzer: SentimentAnalyzer instance
        model_id: Model identifier for logging
        model_name: Human-readable model name
        log_progress: Whether to log progress milestones (default: True)
        
    Returns:
        List of SentimentResult objects
    """
    if not tasks:
        logger.warning("No tasks to analyze")
        return []
    
    # Create coroutines for all tasks
    coroutines = [
        sentiment_analyzer.analyze_task(task, model_id, model_name)
        for task in tasks
    ]
    
    # Execute all tasks concurrently
    # The rate limiter will handle throttling automatically
    results = []
    total_tasks = len(coroutines)
    
    # Process in batches to avoid overwhelming memory
    batch_size = 100
    for i in range(0, total_tasks, batch_size):
        batch = coroutines[i:i + batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        
        # Filter out exceptions and log them
        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                task_idx = i + j
                task = tasks[task_idx] if task_idx < len(tasks) else None
                if task:
                    logger.error(
                        f"Task failed: article_id={task.article_id}, "
                        f"symbol={task.symbol}, component={task.component}, "
                        f"error={result}",
                        exc_info=result
                    )
                else:
                    logger.error(
                        f"Task {task_idx + 1}/{total_tasks} failed: {result}",
                        exc_info=result
                    )
            else:
                results.append(result)
        
        # Log progress milestone (only for BatchOrchestrator)
        if log_progress:
            completed = min(i + batch_size, total_tasks)
            logger.info(
                f"Progress: {completed}/{total_tasks} tasks completed "
                f"({100 * completed // total_tasks}%)"
            )
    
    return results


class BatchOrchestrator:
    """Coordinates end-to-end batch processing workflow for sentiment analysis.
    
    Orchestrates the complete workflow:
    1. Load configuration and initialize AWS clients
    2. Read articles from S3 and load prompt template
    3. Process articles through sentiment analysis with rate limiting
    4. Aggregate results and write to S3 as Parquet files
    """
    
    def __init__(self, config: Config):
        """Initialize batch orchestrator with configuration.
        
        Args:
            config: Configuration object with all execution parameters
        """
        self.config = config
        self.boto3_session = boto3.Session()
        self.s3_client = S3Client(self.boto3_session)
        self.article_processor = ArticleProcessor()
        self.use_parallel_reading = True  # Feature flag for parallel reading
        self.concurrent_limit = 75  # Configurable concurrent download limit
        self.chunk_size = 1000  # Configurable chunk size for batch processing
        
        logger.info(
            f"BatchOrchestrator initialized: "
            f"input_bucket={config.input_s3_bucket}, "
            f"output_bucket={config.output_s3_bucket}, "
            f"models={len(config.models)}, "
            f"parallel_reading={self.use_parallel_reading}"
        )
    
    def run(self) -> None:
        """Execute complete batch processing workflow.
        
        Iterates through all configured models and executes a batch job for each.
        Implements Requirements 4.1, 4.5, 5.1.
        """
        logger.info(f"Starting batch processing workflow with {len(self.config.models)} model(s)")
        
        try:
            # Load system prompt once for all models
            logger.info(f"Loading system prompt from {self.config.system_prompt_s3_uri}")
            try:
                system_prompt = self.s3_client.read_prompt_template(self.config.system_prompt_s3_uri)
                logger.info(
                    f"System prompt loaded successfully: {len(system_prompt)} characters"
                )
            except ClientError as e:
                logger.error(
                    f"Failed to load system prompt from {self.config.system_prompt_s3_uri}: {e}"
                )
                raise
            
            # Load user prompt template once for all models
            logger.info(f"Loading user prompt template from {self.config.prompt_s3_uri}")
            try:
                user_prompt_template = self.s3_client.read_prompt_template(self.config.prompt_s3_uri)
                logger.info(
                    f"User prompt template loaded successfully: {len(user_prompt_template)} characters"
                )
            except ClientError as e:
                logger.error(
                    f"Failed to load user prompt template from {self.config.prompt_s3_uri}: {e}"
                )
                raise
            
            # Process each model configuration (Requirement 5.1)
            for i, model_config in enumerate(self.config.models, 1):
                logger.info(
                    f"Processing model {i}/{len(self.config.models)}: {model_config.get_model_identifier()}"
                )
                
                try:
                    logger.info(
                        f"Starting batch job for model: {model_config.get_model_identifier()} "
                        f"(req/min={model_config.requests_per_minute}, "
                        f"tokens/min={model_config.tokens_per_minute})"
                    )
                    
                    # Execute batch for this model
                    results_df = asyncio.run(
                        self._process_model_batch(model_config, system_prompt, user_prompt_template)
                    )
                    
                    # Save results to S3 (Requirement 4.1, 4.2)
                    self._save_results(results_df, model_config.get_model_identifier())
                    
                    # Log summary statistics
                    successful = results_df['sentiment_score'].notna().sum()
                    failed = results_df['sentiment_score'].isna().sum()
                    logger.info(
                        f"Completed batch job for model {model_config.get_model_identifier()}: "
                        f"{len(results_df)} total results, {successful} successful, {failed} failed"
                    )
                    
                except ClientError as e:
                    logger.error(
                        f"AWS error processing model {model_config.get_model_identifier()}: {e}",
                        exc_info=True
                    )
                    # Continue with next model (Requirement 5.3)
                    continue
                except Exception as e:
                    logger.error(
                        f"Unexpected error processing model {model_config.get_model_identifier()}: {e}",
                        exc_info=True
                    )
                    # Continue with next model (Requirement 5.3)
                    continue
            
            logger.info("Batch processing workflow completed successfully")
            
        except Exception as e:
            logger.error(f"Fatal error in batch processing workflow: {e}", exc_info=True)
            raise
    
    async def _process_model_batch(
        self,
        model_config: ModelConfig,
        system_prompt: str,
        user_prompt_template: str
    ) -> pd.DataFrame:
        """Process all articles for one model configuration with concurrency.
        
        Implements the core processing logic:
        1. Load articles from S3 (parallel or sequential based on feature flag)
        2. Create analysis tasks with filtering (from DataFrame or Iterator)
        3. Analyze tasks concurrently with rate limiting
        4. Aggregate results into DataFrame
        
        Args:
            model_config: Configuration for the model to use
            system_prompt: System prompt for model behavior
            user_prompt_template: User prompt template for sentiment analysis
            
        Returns:
            DataFrame with sentiment analysis results
        """
        logger.info(f"Starting batch processing for model: {model_config.get_model_identifier()}")
        
        # Initialize rate limiter for this model (Requirement 3.1, 3.2)
        rate_limiter = RateLimiter(
            requests_per_minute=model_config.requests_per_minute,
            tokens_per_minute=model_config.tokens_per_minute
        )
        logger.info(
            f"Rate limiter configured: {model_config.requests_per_minute} req/min, "
            f"{model_config.tokens_per_minute} tokens/min"
        )
        
        # Initialize Bedrock client with rate limiter
        bedrock_client = BedrockClient(
            boto3_session=self.boto3_session,
            rate_limiter=rate_limiter
        )
        
        # Initialize sentiment analyzer
        sentiment_analyzer = SentimentAnalyzer(
            bedrock_client=bedrock_client,
            system_prompt=system_prompt,
            user_prompt_template=user_prompt_template,
            rating_scale=self.config.rating_scale,
            job_name=self.config.job_name
        )
        
        # Load articles from S3 (Requirement 2.1, 3.2, 7.2)
        logger.info(f"Loading articles from {self.config.input_s3_bucket}")
        try:
            if self.use_parallel_reading:
                # Use parallel reading to load articles into DataFrame
                logger.info(
                    f"Using parallel reading: concurrent_limit={self.concurrent_limit}, "
                    f"chunk_size={self.chunk_size}"
                )
                articles_df = await self.s3_client.read_articles_parallel(
                    bucket_uri=self.config.input_s3_bucket,
                    concurrent_limit=self.concurrent_limit,
                    chunk_size=self.chunk_size
                )
                
                # Log DataFrame statistics (Requirement 5.4)
                memory_mb = articles_df.memory_usage(deep=True).sum() / (1024 * 1024)
                logger.info(
                    f"DataFrame loaded: rows={len(articles_df)}, "
                    f"memory_mb={memory_mb:.1f}"
                )
                
                # Process DataFrame into analysis tasks with filtering
                logger.info("Processing DataFrame into analysis tasks")
                if self.config.max_articles:
                    logger.info(f"Limiting to {self.config.max_articles} articles")
                tasks = self.article_processor.process_dataframe(
                    articles_df, 
                    max_articles=self.config.max_articles
                )
                logger.info(
                    f"Created {len(tasks)} analysis tasks for model {model_config.get_model_identifier()}"
                )
            else:
                # Fall back to sequential reading (backward compatibility)
                logger.info("Using sequential reading (backward compatibility mode)")
                articles = self.s3_client.read_articles(self.config.input_s3_bucket)
                
                # Process articles into analysis tasks with filtering
                logger.info("Processing articles into analysis tasks")
                if self.config.max_articles:
                    logger.info(f"Limiting to {self.config.max_articles} articles")
                tasks = list(self.article_processor.process_articles(
                    articles, 
                    max_articles=self.config.max_articles
                ))
                logger.info(
                    f"Created {len(tasks)} analysis tasks for model {model_config.get_model_identifier()}"
                )
            
            if not tasks:
                logger.warning(
                    f"No tasks created for model {model_config.get_model_identifier()}. "
                    "Check if articles have valid symbols."
                )
                return pd.DataFrame()
                
        except ClientError as e:
            logger.error(
                f"Failed to load articles from {self.config.input_s3_bucket}: {e}"
            )
            raise
        
        # Analyze tasks concurrently with rate limiting
        logger.info("Starting concurrent sentiment analysis")
        model_name = model_config.model_name if model_config.model_name else model_config.get_model_identifier()
        results = await self._analyze_tasks_concurrently(
            tasks=tasks,
            sentiment_analyzer=sentiment_analyzer,
            model_id=model_config.get_model_identifier(),
            model_name=model_name
        )
        logger.info(f"Completed sentiment analysis: {len(results)} results")
        
        # Aggregate results into DataFrame (Requirement 4.3, 4.4)
        results_df = self._aggregate_results(results)
        
        return results_df
    
    async def _analyze_tasks_concurrently(
        self,
        tasks: List[AnalysisTask],
        sentiment_analyzer: SentimentAnalyzer,
        model_id: str,
        model_name: str
    ) -> List[SentimentResult]:
        """Analyze tasks concurrently with progress logging.
        
        Args:
            tasks: List of analysis tasks to process
            sentiment_analyzer: SentimentAnalyzer instance
            model_id: Model identifier for logging
            model_name: Human-readable model name
            
        Returns:
            List of SentimentResult objects
        """
        return await analyze_tasks_concurrently(
            tasks, sentiment_analyzer, model_id, model_name, log_progress=True
        )
    
    def _aggregate_results(self, results: List[SentimentResult]) -> pd.DataFrame:
        """Aggregate sentiment results into pandas DataFrame with output schema.
        
        Implements the output schema from Requirements 4.3, 4.4:
        - article_id, symbol, component, sentiment_score
        - model_id, timestamp
        - rating_scale_min, rating_scale_max
        
        Args:
            results: List of SentimentResult objects
            
        Returns:
            DataFrame with standardized output schema
        """
        if not results:
            logger.warning("No results to aggregate")
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                'article_id', 'symbol', 'component', 'sentiment_score',
                'model_id', 'model_name', 'timestamp', 'job_name',
                'rating_scale_min', 'rating_scale_max'
            ])
        
        # Convert results to DataFrame
        data = []
        for result in results:
            data.append({
                'article_id': result.article_id,
                'symbol': result.symbol,
                'component': result.component,
                'sentiment_score': result.sentiment_score,
                'model_id': result.model_id,
                'model_name': result.model_name,
                'timestamp': result.timestamp,
                'job_name': result.job_name,
                'rating_scale_min': self.config.rating_scale.min,
                'rating_scale_max': self.config.rating_scale.max
            })
        
        df = pd.DataFrame(data)
        
        logger.info(
            f"Aggregated {len(df)} results: "
            f"{df['sentiment_score'].notna().sum()} with scores, "
            f"{df['sentiment_score'].isna().sum()} without scores"
        )
        
        return df
    
    def _save_results(self, results: pd.DataFrame, model_id: str) -> None:
        """Write Parquet files to output S3 bucket with Hive-style partitioning.
        
        Partitions by job_name and model_name:
        s3://bucket/job=<job_name>/model=<model_name>/data.parquet
        
        Implements Requirements 4.1, 4.2, 4.5.
        
        Args:
            results: DataFrame with sentiment analysis results
            model_id: Model identifier to include in filename
        """
        if results.empty:
            logger.warning(f"No results to save for model {model_id}")
            return
        
        # Find the model config to get model_name
        model_config = next((m for m in self.config.models if m.model_id == model_id), None)
        
        # Use model_name if provided, otherwise use sanitized model_id
        if model_config and model_config.model_name:
            model_name = model_config.model_name
        else:
            # Use safe filename format: replace special characters
            model_name = model_id.replace('/', '_').replace(':', '_').replace('.', '_')
        
        # Build partitioned path: job=<job_name>/model=<model_name>/
        job_name = self.config.job_name if self.config.job_name else 'default'
        partition_path = f"job={job_name}/model={model_name}/"
        filename = "data.parquet"
        
        # Full path with partitioning
        full_path = f"{partition_path}{filename}"
        
        logger.info(
            f"Saving {len(results)} results to {self.config.output_s3_bucket}/{full_path}"
        )
        
        try:
            # Write Parquet file to S3 with partitioning (Requirement 4.1, 4.2)
            self.s3_client.write_parquet(
                df=results,
                bucket_uri=self.config.output_s3_bucket,
                filename=full_path
            )
            
            logger.info(
                f"Successfully saved results for model {model_id}: "
                f"path={full_path}, size={len(results)} rows"
            )
            
        except ClientError as e:
            logger.error(
                f"AWS error saving results for model {model_id}: {e}",
                exc_info=True
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error saving results for model {model_id}: {e}",
                exc_info=True
            )
            raise



class StreamingBatchOrchestrator:
    """Orchestrates streaming batch processing with S3 Tables output.
    
    Implements a streaming architecture that processes articles in configurable
    batches (read → process → write → repeat) to reduce memory footprint and
    enable incremental processing.
    
    Each model writes to its own S3 Table to avoid concurrent write conflicts.
    Table names are derived from model_name: sentiment-results-{model_name}
    """
    
    def __init__(self, config: Config):
        """Initialize streaming batch orchestrator.
        
        Args:
            config: Configuration object with streaming batch parameters
        """
        self.config = config
        self.boto3_session = boto3.Session()
        self.s3_client = S3Client(self.boto3_session)
        self.article_processor = ArticleProcessor()
        self.batch_size = config.batch_size
        
        # S3 Tables writer is created per-model to avoid concurrent write conflicts
        # Each model gets its own table: sentiment-results-{model_name}
        self.s3_tables_writer = None
        
        logger.info(
            f"StreamingBatchOrchestrator initialized: "
            f"input_bucket={config.input_s3_bucket}, "
            f"batch_size={self.batch_size}, "
            f"write_mode={config.write_mode}, "
            f"models={len(config.models)}, "
            f"table_per_model=True"
        )
    
    def _get_table_name_for_model(self, model_config: ModelConfig) -> str:
        """Generate table name for a specific model.
        
        Creates a unique table name per model to avoid concurrent write conflicts.
        Format: sentiment_results_{model_name}
        
        S3 Tables naming rules:
        - Must be lowercase
        - Can only contain alphanumeric characters and underscores
        - Cannot start or end with underscore
        - No consecutive underscores
        
        Args:
            model_config: Model configuration
            
        Returns:
            Table name string (sanitized for S3 Tables naming rules)
        """
        # Use model_name if provided, otherwise derive from model_id/model_arn
        if model_config.model_name:
            base_name = model_config.model_name
        else:
            # Extract a clean name from model identifier
            identifier = model_config.get_model_identifier()
            if identifier.startswith('arn:'):
                # For ARNs, use the last part after the final slash
                # e.g., "arn:aws:bedrock:us-east-1:123456789012:provisioned-model/abc123" -> "abc123"
                base_name = identifier.split('/')[-1]
            else:
                # For model IDs, use the part after the last dot and before the colon
                # e.g., "us.anthropic.claude-3-haiku-20240307-v1:0" -> "claude-3-haiku-20240307-v1"
                base_name = identifier.split('.')[-1].split(':')[0]
        
        # Sanitize for S3 Tables naming: lowercase, alphanumeric and underscores only
        sanitized = base_name.lower()
        # Replace any non-alphanumeric character (except underscore) with underscore
        sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in sanitized)
        # Remove consecutive underscores
        sanitized = '_'.join(filter(None, sanitized.split('_')))
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure we have a valid name (fallback to 'model' if empty after sanitization)
        if not sanitized:
            sanitized = 'model'
        
        return f"sentiment_results_{sanitized}"
        
        return f"sentiment_results_{sanitized}"
    
    def _create_s3_tables_writer_for_model(
        self, model_config: ModelConfig
    ) -> S3TablesWriter:
        """Create an S3TablesWriter for a specific model.
        
        Each model gets its own table to avoid concurrent write conflicts
        when multiple jobs run in parallel.
        
        Args:
            model_config: Model configuration
            
        Returns:
            S3TablesWriter instance for the model's table
        """
        table_name = self._get_table_name_for_model(model_config)
        
        logger.info(
            f"Creating S3TablesWriter for model {model_config.get_model_identifier()}: "
            f"table_name={table_name}"
        )
        
        return S3TablesWriter(
            catalog_name=self.config.catalog_name,
            table_name=table_name,
            warehouse_path=self.config.s3_table_warehouse,
            rating_scale_min=self.config.rating_scale.min,
            rating_scale_max=self.config.rating_scale.max
        )
    
    def run(self) -> None:
        """Execute complete streaming batch processing workflow.
        
        Iterates through all configured models and executes streaming batch
        processing for each.
        """
        logger.info(
            f"Starting streaming batch workflow with {len(self.config.models)} model(s)"
        )
        
        try:
            # Load system prompt once for all models
            logger.info(f"Loading system prompt from {self.config.system_prompt_s3_uri}")
            try:
                system_prompt = self.s3_client.read_prompt_template(
                    self.config.system_prompt_s3_uri
                )
                logger.info(
                    f"System prompt loaded successfully: {len(system_prompt)} characters"
                )
            except ClientError as e:
                logger.error(
                    f"Failed to load system prompt from "
                    f"{self.config.system_prompt_s3_uri}: {e}"
                )
                raise
            
            # Load user prompt template once for all models
            logger.info(f"Loading user prompt template from {self.config.prompt_s3_uri}")
            try:
                user_prompt_template = self.s3_client.read_prompt_template(
                    self.config.prompt_s3_uri
                )
                logger.info(
                    f"User prompt template loaded successfully: "
                    f"{len(user_prompt_template)} characters"
                )
            except ClientError as e:
                logger.error(
                    f"Failed to load user prompt template from "
                    f"{self.config.prompt_s3_uri}: {e}"
                )
                raise
            
            # Process each model configuration
            for i, model_config in enumerate(self.config.models, 1):
                logger.info(
                    f"Processing model {i}/{len(self.config.models)}: "
                    f"{model_config.get_model_identifier()}"
                )
                
                try:
                    logger.info(
                        f"Starting streaming batch job for model: {model_config.get_model_identifier()} "
                        f"(req/min={model_config.requests_per_minute}, "
                        f"tokens/min={model_config.tokens_per_minute})"
                    )
                    
                    # Execute streaming batch for this model
                    stats = asyncio.run(
                        self._process_model_streaming(
                            model_config, system_prompt, user_prompt_template
                        )
                    )
                    
                    # Log summary statistics
                    logger.info(
                        f"Completed streaming batch job for model {model_config.get_model_identifier()}: "
                        f"total_articles={stats['total_articles']}, "
                        f"total_tasks={stats['total_tasks']}, "
                        f"successful={stats['successful']}, "
                        f"failed={stats['failed']}, "
                        f"batches={stats['batches']}, "
                        f"total_time={stats['total_time']:.1f}s"
                    )
                    
                except ClientError as e:
                    logger.error(
                        f"AWS error processing model {model_config.get_model_identifier()}: {e}",
                        exc_info=True
                    )
                    # Continue with next model
                    continue
                except Exception as e:
                    logger.error(
                        f"Unexpected error processing model {model_config.get_model_identifier()}: {e}",
                        exc_info=True
                    )
                    # Continue with next model
                    continue
            
            logger.info("Streaming batch workflow completed successfully")
            
        except Exception as e:
            logger.error(
                f"Fatal error in streaming batch workflow: {e}", exc_info=True
            )
            raise
    
    async def _process_model_streaming(
        self,
        model_config: ModelConfig,
        system_prompt: str,
        user_prompt_template: str
    ) -> Dict[str, int]:
        """Process articles in streaming batches for one model.
        
        Implements the streaming batch loop:
        1. Read batch of articles from S3
        2. Process batch through sentiment analysis
        3. Write results to S3 Tables
        4. Repeat until all articles processed
        
        Args:
            model_config: Configuration for the model to use
            system_prompt: System prompt for model behavior
            user_prompt_template: User prompt template for sentiment analysis
            
        Returns:
            Dict with processing statistics
        """
        logger.info(
            f"Starting streaming batch processing for model: {model_config.get_model_identifier()}"
        )
        
        start_time = time.time()
        
        # Create model-specific S3 Tables writer to avoid concurrent write conflicts
        s3_tables_writer = None
        if self.config.enable_streaming_batch:
            s3_tables_writer = self._create_s3_tables_writer_for_model(model_config)
            table_name = self._get_table_name_for_model(model_config)
            logger.info(
                f"Using model-specific S3 Table: {table_name} for {model_config.get_model_identifier()}"
            )
        
        # Count total articles for logging (Requirement 9.1)
        estimated_total_articles = self.s3_client.count_articles(
            self.config.input_s3_bucket
        )
        if self.config.max_articles is not None:
            estimated_total_articles = min(
                estimated_total_articles, self.config.max_articles
            )
        
        # Log streaming start with total articles and batch size (Requirement 9.1)
        logger.info(
            f"Streaming batch start: total_articles={estimated_total_articles}, "
            f"batch_size={self.batch_size}, write_mode={self.config.write_mode}, "
            f"model_id={model_config.get_model_identifier()}"
        )
        
        # Initialize rate limiter for this model
        rate_limiter = RateLimiter(
            requests_per_minute=model_config.requests_per_minute,
            tokens_per_minute=model_config.tokens_per_minute
        )
        logger.info(
            f"Rate limiter configured: {model_config.requests_per_minute} req/min, "
            f"{model_config.tokens_per_minute} tokens/min"
        )
        
        # Initialize Bedrock client with rate limiter
        bedrock_client = BedrockClient(
            boto3_session=self.boto3_session,
            rate_limiter=rate_limiter
        )
        
        # Initialize sentiment analyzer
        sentiment_analyzer = SentimentAnalyzer(
            bedrock_client=bedrock_client,
            system_prompt=system_prompt,
            user_prompt_template=user_prompt_template,
            rating_scale=self.config.rating_scale,
            job_name=self.config.job_name
        )
        
        # Cumulative statistics across all batches
        total_articles = 0
        total_tasks = 0
        total_successful = 0
        total_failed = 0
        batch_num = 0
        
        # Read articles in batches and process
        logger.info(f"Starting batch reading from {self.config.input_s3_bucket}")
        
        try:
            article_batches = self.s3_client.read_articles_batch(
                bucket_uri=self.config.input_s3_bucket,
                batch_size=self.batch_size
            )
            
            for article_batch in article_batches:
                batch_num += 1
                batch_start_time = time.time()
                
                # Check if we've reached max_articles limit
                if self.config.max_articles is not None:
                    remaining = self.config.max_articles - total_articles
                    if remaining <= 0:
                        logger.info(
                            f"Reached max_articles limit of {self.config.max_articles}, "
                            f"stopping processing"
                        )
                        break
                    if len(article_batch) > remaining:
                        article_batch = article_batch[:remaining]
                        logger.info(
                            f"Limiting batch to {remaining} articles to respect "
                            f"max_articles={self.config.max_articles}"
                        )
                
                logger.info(
                    f"Processing batch {batch_num}: {len(article_batch)} articles"
                )
                
                # Convert articles to DataFrame for processing
                batch_df = pd.DataFrame(article_batch)
                
                # Process articles into analysis tasks
                tasks = self.article_processor.process_dataframe(batch_df)
                
                if not tasks:
                    logger.warning(
                        f"Batch {batch_num}: No tasks created. "
                        "Check if articles have valid symbols."
                    )
                    total_articles += len(article_batch)
                    continue
                
                logger.info(
                    f"Batch {batch_num}: Created {len(tasks)} analysis tasks"
                )
                
                # Analyze tasks concurrently with rate limiting
                model_name = (
                    model_config.model_name 
                    if model_config.model_name 
                    else model_config.get_model_identifier()
                )
                results = await self._analyze_tasks_concurrently(
                    tasks=tasks,
                    sentiment_analyzer=sentiment_analyzer,
                    model_id=model_config.get_model_identifier(),
                    model_name=model_name
                )
                
                # Count successful and failed results
                batch_successful = sum(
                    1 for r in results if r.sentiment_score is not None
                )
                batch_failed = len(results) - batch_successful
                
                logger.info(
                    f"Batch {batch_num}: Analysis complete - "
                    f"{batch_successful} successful, {batch_failed} failed"
                )
                
                # Write results to S3 Tables (using model-specific writer)
                if results and s3_tables_writer:
                    write_stats = s3_tables_writer.write_results_batch(
                        results=results,
                        mode=self.config.write_mode
                    )
                    logger.info(
                        f"Batch {batch_num}: Wrote {write_stats['written']} results "
                        f"to S3 Tables (mode={self.config.write_mode})"
                    )
                
                # Update cumulative statistics
                total_articles += len(article_batch)
                total_tasks += len(tasks)
                total_successful += batch_successful
                total_failed += batch_failed
                
                # Log progress after each batch with articles processed and time (Requirement 9.2)
                batch_elapsed = time.time() - batch_start_time
                total_elapsed = time.time() - start_time
                logger.info(
                    f"Batch progress: batch={batch_num}, batch_time={batch_elapsed:.1f}s, "
                    f"batch_articles={len(article_batch)}, batch_tasks={len(tasks)}, "
                    f"batch_successful={batch_successful}, batch_failed={batch_failed} | "
                    f"Cumulative: total_articles={total_articles}, total_tasks={total_tasks}, "
                    f"total_successful={total_successful}, total_failed={total_failed}, "
                    f"total_elapsed={total_elapsed:.1f}s"
                )
                
                # Check if we've reached max_articles limit
                if (self.config.max_articles is not None and 
                    total_articles >= self.config.max_articles):
                    logger.info(
                        f"Reached max_articles limit of {self.config.max_articles}, "
                        f"stopping processing"
                    )
                    break
            
            total_time = time.time() - start_time
            
            # Log streaming completion with total time and success/failure counts (Requirement 9.4)
            success_rate = (
                (total_successful / total_tasks * 100) if total_tasks > 0 else 0
            )
            logger.info(
                f"Streaming batch completion: model_id={model_config.get_model_identifier()}, "
                f"total_time={total_time:.1f}s, batches={batch_num}, "
                f"total_articles={total_articles}, total_tasks={total_tasks}, "
                f"successful={total_successful}, failed={total_failed}, "
                f"success_rate={success_rate:.1f}%"
            )
            
            return {
                'total_articles': total_articles,
                'total_tasks': total_tasks,
                'successful': total_successful,
                'failed': total_failed,
                'batches': batch_num,
                'total_time': total_time
            }
            
        except ClientError as e:
            logger.error(
                f"Failed to read articles from {self.config.input_s3_bucket}: {e}"
            )
            raise
    
    async def _analyze_tasks_concurrently(
        self,
        tasks: List[AnalysisTask],
        sentiment_analyzer: SentimentAnalyzer,
        model_id: str,
        model_name: str
    ) -> List[SentimentResult]:
        """Analyze tasks concurrently (no progress logging - batch-level logging used).
        
        Args:
            tasks: List of analysis tasks to process
            sentiment_analyzer: SentimentAnalyzer instance
            model_id: Model identifier for logging
            model_name: Human-readable model name
            
        Returns:
            List of SentimentResult objects
        """
        # Don't log progress here - streaming orchestrator logs at batch level
        return await analyze_tasks_concurrently(
            tasks, sentiment_analyzer, model_id, model_name, log_progress=False
        )
