"""Bedrock API client module with retry logic."""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Set, Dict, Any

from config import RatingScale
from rate_limiter import RateLimiter


logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior with exponential backoff.
    
    Attributes:
        max_attempts: Maximum number of retry attempts
        backoff_multiplier: Multiplier for exponential backoff
        initial_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay in seconds between retries
        retryable_status_codes: HTTP status codes that should trigger retries
            - 408: Request Timeout (ModelTimeoutException)
            - 429: Too Many Requests (Throttling)
            - 500: Internal Server Error
            - 503: Service Unavailable
    """
    max_attempts: int = 3
    backoff_multiplier: float = 2.0
    initial_delay: float = 1.0
    max_delay: float = 60.0
    retryable_status_codes: Set[int] = field(default_factory=lambda: {408, 429, 500, 503})


class BedrockClient:
    """Client for AWS Bedrock API with rate limiting and retry logic.
    
    Handles sentiment analysis requests to Bedrock models with:
    - Rate limiting integration
    - Exponential backoff retry for transient errors
    - Token estimation for proactive rate limiting
    - Response parsing and error handling
    """
    
    def __init__(self, boto3_session, rate_limiter: RateLimiter, retry_config: Optional[RetryConfig] = None):
        """Initialize Bedrock client.
        
        Args:
            boto3_session: Boto3 session for AWS API calls
            rate_limiter: RateLimiter instance for enforcing API limits
            retry_config: Optional retry configuration (uses defaults if not provided)
        """
        self.bedrock_runtime = boto3_session.client('bedrock-runtime')
        self.rate_limiter = rate_limiter
        self.retry_config = retry_config or RetryConfig()
        
        logger.info(
            f"BedrockClient initialized with retry config: "
            f"max_attempts={self.retry_config.max_attempts}, "
            f"retryable_codes={self.retry_config.retryable_status_codes}"
        )
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count for rate limiting.
        
        Uses a simple heuristic: approximately 4 characters per token.
        This is a conservative estimate for English text.
        
        Args:
            text: Text to estimate tokens for
            
        Returns:
            Estimated number of tokens
        """
        if not text:
            return 0
        
        # Rough estimate: 4 characters per token
        # Add some buffer for prompt overhead
        estimated = len(text) // 4 + 100  # 100 tokens for prompt template overhead
        estimated = max(estimated, 1)
        
        logger.debug(f"Estimated tokens: {estimated} (text length: {len(text)} chars)")
        
        return estimated
    
    def _parse_sentiment_score(self, response: Dict[str, Any], rating_scale: RatingScale) -> Optional[float]:
        """Extract sentiment score from Bedrock Converse API response.
        
        Parses the Converse API response and extracts a numerical sentiment score.
        The Converse API has a unified response format across all models.
        
        Args:
            response: Raw response from Bedrock Converse API
            rating_scale: Rating scale configuration for validation
            
        Returns:
            Extracted sentiment score as float, or None if parsing fails
        """
        try:
            # Converse API response format
            output = response.get('output')
            if not output:
                logger.warning("No output in Bedrock Converse response")
                return None
            
            message = output.get('message')
            if not message:
                logger.warning("No message in Converse response output")
                return None
            
            content = message.get('content', [])
            if not content or len(content) == 0:
                logger.warning("No content in Converse response message")
                return None
            
            # Extract text from first content block
            text = content[0].get('text', '')
            if not text:
                logger.warning("No text in Converse response content")
                return None
            
            # Extract numerical score from text
            import re
            numbers = re.findall(r'-?\d+\.?\d*', text)
            
            if not numbers:
                logger.warning(f"No numerical score found in response: {text}")
                return None
            
            # Take the first number as the score
            score = float(numbers[0])
            
            # Validate score is within rating scale
            if score < rating_scale.min or score > rating_scale.max:
                logger.warning(
                    f"Score {score} outside rating scale [{rating_scale.min}, {rating_scale.max}]"
                )
                return None
            
            return score
            
        except Exception as e:
            logger.error(f"Error parsing sentiment score: {e}")
            return None
    
    async def analyze_sentiment(
        self,
        model_identifier: str,
        system_prompt: str,
        user_prompt: str,
        text: str,
        rating_scale: RatingScale
    ) -> Optional[float]:
        """Call Bedrock Converse API with rate limiting and retry logic.
        
        Analyzes sentiment of the given text using the specified model.
        Uses the unified Converse API which works across all Bedrock models.
        Implements exponential backoff retry for transient errors.
        
        Args:
            model_identifier: Bedrock model identifier (model ID or model ARN)
            system_prompt: System prompt for model behavior
            user_prompt: User prompt for sentiment analysis
            text: Text to analyze
            rating_scale: Rating scale for score validation
            
        Returns:
            Sentiment score as float, or None if analysis fails
        """
        # Estimate tokens for rate limiting
        estimated_tokens = self._estimate_tokens(system_prompt + user_prompt + text)
        
        logger.debug(
            f"Starting sentiment analysis: model_identifier={model_identifier}, "
            f"estimated_tokens={estimated_tokens}"
        )
        
        # Acquire rate limit capacity
        await self.rate_limiter.acquire(estimated_tokens)
        
        # Retry loop with exponential backoff
        delay = self.retry_config.initial_delay
        last_error = None
        
        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                # Build Converse API request
                # Unified format works for all models
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "text": f"{user_prompt}\n\nText to analyze:\n{text}"
                            }
                        ]
                    }
                ]
                
                # System prompt configuration
                system = [
                    {
                        "text": system_prompt
                    }
                ]
                
                # Inference configuration
                inference_config = {
                    "maxTokens": 100,
                    "temperature": 0.0  # Deterministic for consistent scoring
                    # Note: topP is omitted as some models don't allow both temperature and topP
                }
                
                # Call Bedrock Converse API in a thread pool to avoid blocking the event loop
                logger.debug(f"Invoking Bedrock Converse API: {model_identifier}")
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,  # default ThreadPoolExecutor
                    lambda: self.bedrock_runtime.converse(
                        modelId=model_identifier,
                        messages=messages,
                        system=system,
                        inferenceConfig=inference_config
                    )
                )
                
                # Parse and return sentiment score
                score = self._parse_sentiment_score(response, rating_scale)
                
                if score is not None:
                    logger.debug(
                        f"Successfully analyzed sentiment: model_identifier={model_identifier}, score={score}"
                    )
                else:
                    logger.warning(
                        f"Failed to parse sentiment score from response: model_identifier={model_identifier}"
                    )
                
                return score
                
            except Exception as e:
                last_error = e
                error_code = self._get_error_code(e)
                
                # Check if error is retryable
                if error_code in self.retry_config.retryable_status_codes:
                    if attempt < self.retry_config.max_attempts:
                        logger.warning(
                            f"Retryable error (code {error_code}) on attempt {attempt}/{self.retry_config.max_attempts}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        await asyncio.sleep(delay)
                        delay = min(delay * self.retry_config.backoff_multiplier, self.retry_config.max_delay)
                        continue
                    else:
                        logger.error(
                            f"Max retry attempts ({self.retry_config.max_attempts}) reached for retryable error: {e}"
                        )
                else:
                    # Non-retryable error (400, 403, etc.)
                    logger.error(f"Non-retryable error (code {error_code}): {e}")
                    break
        
        # All retries exhausted or non-retryable error
        logger.error(f"Failed to analyze sentiment after {self.retry_config.max_attempts} attempts: {last_error}")
        return None
    
    def _get_error_code(self, error: Exception) -> Optional[int]:
        """Extract HTTP status code from exception.
        
        Args:
            error: Exception from Bedrock API call
            
        Returns:
            HTTP status code if available, None otherwise
        """
        # Check for boto3 ClientError
        if hasattr(error, 'response'):
            response = error.response
            if isinstance(response, dict):
                http_status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
                if http_status:
                    return http_status
                
                # Also check Error.Code for specific error types
                error_code = response.get('Error', {}).get('Code')
                if error_code:
                    # Map common error codes to HTTP status codes
                    error_code_map = {
                        'ThrottlingException': 429,
                        'TooManyRequestsException': 429,
                        'ServiceUnavailableException': 503,
                        'InternalServerError': 500,
                        'ModelTimeoutException': 408,
                        'ValidationException': 400,
                        'AccessDeniedException': 403,
                        'ResourceNotFoundException': 404
                    }
                    return error_code_map.get(error_code)
        
        return None
