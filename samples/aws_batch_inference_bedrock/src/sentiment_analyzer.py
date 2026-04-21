"""Sentiment analyzer orchestration module."""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from article_processor import AnalysisTask
from bedrock_client import BedrockClient
from config import RatingScale


logger = logging.getLogger(__name__)


@dataclass
class SentimentResult:
    """Result of sentiment analysis for a single task.
    
    Attributes:
        article_id: Unique identifier for the article
        symbol: Stock ticker symbol
        component: Component type ('content', 'headline', or 'summary')
        sentiment_score: Numerical sentiment score, or None if text was empty or analysis failed
        model_id: Bedrock model identifier used for analysis
        model_name: Human-readable model name for easier querying
        timestamp: Article publication date (from created_at field)
        job_name: Optional job identifier for tracking runs
        error: Error message if analysis failed, None otherwise
    """
    article_id: str
    symbol: str
    component: str
    sentiment_score: Optional[float]
    model_id: str
    model_name: str
    timestamp: datetime
    job_name: Optional[str] = None
    error: Optional[str] = None


class SentimentAnalyzer:
    """Orchestrates sentiment analysis for tasks using Bedrock API.
    
    Handles prompt formatting, empty text detection, and result aggregation.
    """
    
    def __init__(
        self,
        bedrock_client: BedrockClient,
        system_prompt: str,
        user_prompt_template: str,
        rating_scale: RatingScale,
        job_name: Optional[str] = None
    ):
        """Initialize sentiment analyzer.
        
        Args:
            bedrock_client: BedrockClient instance for API calls
            system_prompt: System prompt for model behavior
            user_prompt_template: Template string for user prompts
            rating_scale: Rating scale configuration for sentiment scores
            job_name: Optional job identifier for tracking runs
        """
        self.bedrock_client = bedrock_client
        self.system_prompt = system_prompt
        self.user_prompt_template = user_prompt_template
        self.rating_scale = rating_scale
        self.job_name = job_name
        
        logger.info(
            f"SentimentAnalyzer initialized: "
            f"rating_scale=[{rating_scale.min}, {rating_scale.max}], "
            f"system_prompt_length={len(system_prompt)}, "
            f"job_name={job_name if job_name else 'none'}"
        )
    
    def _build_user_prompt(self, task: AnalysisTask) -> str:
        """Build user prompt from template with task context and rating scale.
        
        Formats the user prompt template with:
        - Article ID and symbol for context
        - Component type being analyzed
        - Rating scale information (min, max)
        
        Args:
            task: AnalysisTask containing article and component information
            
        Returns:
            Formatted user prompt string ready for Bedrock API
        """
        # Format the user prompt template with task context and rating scale
        user_prompt = self.user_prompt_template.format(
            article_id=task.article_id,
            symbol=task.symbol,
            component=task.component,
            rating_min=self.rating_scale.min,
            rating_max=self.rating_scale.max
        )
        
        return user_prompt
    
    async def analyze_task(
        self,
        task: AnalysisTask,
        model_identifier: str,
        model_name: str
    ) -> SentimentResult:
        """Analyze single task, returning result with score or null.
        
        Implements the following logic:
        - Returns null score for empty text without calling Bedrock (Requirement 2.3)
        - Calls Bedrock API for non-empty text (Requirement 2.5)
        - Captures errors and returns result with error message
        
        Args:
            task: AnalysisTask to analyze
            model_identifier: Bedrock model identifier (model ID or model ARN) to use
            model_name: Human-readable model name for output
            
        Returns:
            SentimentResult with score (or None) and metadata
        """
        # Use article's created_at timestamp if available, otherwise use current time
        if task.created_at:
            try:
                # Parse ISO 8601 format: "2019-07-08T16:49:02Z"
                timestamp = datetime.fromisoformat(task.created_at.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                logger.warning(
                    f"Invalid created_at format: {task.created_at}, using current time"
                )
                timestamp = datetime.utcnow()
        else:
            timestamp = datetime.utcnow()
        
        # Handle empty text without calling Bedrock (Requirement 2.3, 2.4)
        if task.text is None or task.text.strip() == '':
            logger.warning(
                f"Skipping analysis for empty text: "
                f"article_id={task.article_id}, symbol={task.symbol}, "
                f"component={task.component}, model_identifier={model_identifier}"
            )
            return SentimentResult(
                article_id=task.article_id,
                symbol=task.symbol,
                component=task.component,
                sentiment_score=None,
                model_id=model_identifier,
                model_name=model_name,
                timestamp=timestamp,
                job_name=self.job_name,
                error=None
            )
        
        # Call Bedrock API for non-empty text (Requirement 2.5)
        try:
            user_prompt = self._build_user_prompt(task)
            
            logger.debug(
                f"Analyzing sentiment: article_id={task.article_id}, "
                f"symbol={task.symbol}, component={task.component}, "
                f"model_identifier={model_identifier}, text_length={len(task.text)}"
            )
            
            sentiment_score = await self.bedrock_client.analyze_sentiment(
                model_identifier=model_identifier,
                system_prompt=self.system_prompt,
                user_prompt=user_prompt,
                text=task.text,
                rating_scale=self.rating_scale
            )
            
            if sentiment_score is not None:
                logger.info(
                    f"Sentiment analysis successful: article_id={task.article_id}, "
                    f"symbol={task.symbol}, component={task.component}, "
                    f"model_identifier={model_identifier}, score={sentiment_score}"
                )
            else:
                logger.warning(
                    f"Failed to extract sentiment score: article_id={task.article_id}, "
                    f"symbol={task.symbol}, component={task.component}, model_identifier={model_identifier}"
                )
            
            return SentimentResult(
                article_id=task.article_id,
                symbol=task.symbol,
                component=task.component,
                sentiment_score=sentiment_score,
                model_id=model_identifier,
                model_name=model_name,
                timestamp=timestamp,
                job_name=self.job_name,
                error=None if sentiment_score is not None else "Failed to extract sentiment score"
            )
            
        except Exception as e:
            logger.error(
                f"Sentiment analysis failed: article_id={task.article_id}, "
                f"symbol={task.symbol}, component={task.component}, "
                f"model_identifier={model_identifier}, error={str(e)}",
                exc_info=True
            )
            
            return SentimentResult(
                article_id=task.article_id,
                symbol=task.symbol,
                component=task.component,
                sentiment_score=None,
                model_id=model_identifier,
                model_name=model_name,
                timestamp=timestamp,
                job_name=self.job_name,
                error=str(e)
            )
