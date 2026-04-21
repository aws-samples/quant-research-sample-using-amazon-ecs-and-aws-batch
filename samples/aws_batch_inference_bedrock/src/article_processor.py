"""Article processing and filtering logic for sentiment analysis."""

import logging
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Union

import pandas as pd


logger = logging.getLogger(__name__)


@dataclass
class AnalysisTask:
    """Represents a single sentiment analysis task for an article-symbol-component combination."""
    article_id: str
    symbol: str
    component: str  # 'content', 'headline', or 'summary'
    text: Optional[str]
    created_at: Optional[str] = None  # Article publication date (ISO 8601 format)


class ArticleProcessor:
    """Processes articles and creates analysis tasks with filtering logic."""
    
    def process_dataframe(self, df: pd.DataFrame, max_articles: Optional[int] = None) -> List[AnalysisTask]:
        """
        Convert DataFrame of articles into analysis tasks in bulk.
        
        Args:
            df: DataFrame with columns: id, symbols, content, headline, summary, created_at
            max_articles: Maximum number of articles to process (None for unlimited)
            
        Returns:
            List of AnalysisTask objects
        """
        total_articles = len(df)
        articles_to_process = total_articles
        
        # Apply max_articles limit if specified
        if max_articles is not None and max_articles < total_articles:
            df = df.head(max_articles)
            articles_to_process = max_articles
            logger.info(f"Limiting processing to {max_articles} articles out of {total_articles}")
        
        # Pre-allocate task list with estimated size
        # Each article can have multiple symbols and 3 components
        estimated_tasks = articles_to_process * 3  # Conservative estimate
        tasks = []
        tasks_capacity = estimated_tasks
        
        skipped_articles = 0
        
        # Process each row in the DataFrame
        for idx, row in df.iterrows():
            row_tasks = self._extract_tasks_from_row(row)
            
            if not row_tasks:
                skipped_articles += 1
                article_id = row.get('id', 'unknown')
                logger.warning(
                    f"Skipped article with empty symbols: article_id={article_id}"
                )
            else:
                tasks.extend(row_tasks)
        
        logger.info(
            f"DataFrame processing complete: {articles_to_process} articles processed, "
            f"{skipped_articles} skipped, {len(tasks)} tasks created"
        )
        
        return tasks
    
    def _extract_tasks_from_row(self, row: pd.Series) -> List[AnalysisTask]:
        """
        Extract all valid symbol-component combinations from DataFrame row.
        
        Implements the same filtering logic as _extract_tasks_from_article:
        - Skip articles with empty symbols array (Requirement 2.1)
        - Create separate tasks for each symbol (Requirement 2.2)
        - Process all three components: content, headline, summary (Requirement 2.4)
        - Assign text=None for empty/null component fields (Requirement 2.3)
        
        Args:
            row: DataFrame row with article data
            
        Returns:
            List of AnalysisTask objects
        """
        tasks = []
        
        # Get article_id (field is 'id' in the DataFrame)
        article_id = str(row.get('id', ''))
        
        # Get created_at timestamp
        created_at = row.get('created_at')
        if pd.isna(created_at):
            created_at = None
        
        # Get symbols array
        symbols = row.get('symbols', [])
        
        # Skip articles with empty symbols array (Requirement 2.1)
        if not symbols or (isinstance(symbols, list) and len(symbols) == 0):
            logger.debug(f"Skipping article with empty symbols: article_id={article_id}")
            return tasks
        
        # Define the three components to process (Requirement 2.4)
        components = ['content', 'headline', 'summary']
        
        # Create separate task for each symbol-component combination (Requirement 2.2)
        for symbol in symbols:
            for component in components:
                # Get component text, using None for empty/null fields (Requirement 2.3)
                text = row.get(component)
                
                # Handle pandas NA/NaN values
                if pd.isna(text):
                    text = None
                # Convert empty strings to None
                elif text == '':
                    text = None
                
                if text is None:
                    logger.debug(
                        f"Empty component field: article_id={article_id}, "
                        f"symbol={symbol}, component={component}"
                    )
                
                # Create analysis task
                task = AnalysisTask(
                    article_id=article_id,
                    symbol=symbol,
                    component=component,
                    text=text,
                    created_at=created_at
                )
                tasks.append(task)
        
        logger.debug(
            f"Created {len(tasks)} tasks for article: article_id={article_id}, "
            f"symbols={len(symbols)}"
        )
        
        return tasks
    
    def process_articles(self, articles: Iterator[Dict], max_articles: Optional[int] = None) -> Iterator[AnalysisTask]:
        """
        Convert articles into individual analysis tasks.
        
        Filters out articles with empty symbols arrays and expands multi-symbol
        articles into separate tasks per symbol-component combination.
        
        Args:
            articles: Iterator of article dictionaries from S3
            max_articles: Maximum number of articles to process (None for unlimited)
            
        Yields:
            AnalysisTask objects for each valid article-symbol-component combination
        """
        total_articles = 0
        skipped_articles = 0
        total_tasks = 0
        
        for article in articles:
            # Stop if we've reached the limit
            if max_articles is not None and total_articles >= max_articles:
                logger.info(f"Reached max_articles limit of {max_articles}, stopping article processing")
                break
            
            total_articles += 1
            tasks = self._extract_tasks_from_article(article)
            
            if not tasks:
                skipped_articles += 1
                article_id = article.get('id', 'unknown')
                logger.warning(
                    f"Skipped article with empty symbols: article_id={article_id}"
                )
            else:
                total_tasks += len(tasks)
                for task in tasks:
                    yield task
        
        logger.info(
            f"Article processing complete: {total_articles} articles processed, "
            f"{skipped_articles} skipped, {total_tasks} tasks created"
        )
    
    def _extract_tasks_from_article(self, article: Dict) -> List[AnalysisTask]:
        """
        Extract all valid symbol-component combinations from an article.
        
        Implements the following logic:
        - Skip articles with empty symbols array (Requirement 2.1)
        - Create separate tasks for each symbol (Requirement 2.2)
        - Process all three components: content, headline, summary (Requirement 2.4)
        - Assign text=None for empty/null component fields (Requirement 2.3)
        
        Args:
            article: Article dictionary with article_id, symbols, and component fields
            
        Returns:
            List of AnalysisTask objects
        """
        tasks = []
        
        # Get article_id (field is 'id' in the actual data structure)
        article_id = str(article.get('id', ''))
        
        # Get created_at timestamp
        created_at = article.get('created_at')
        
        # Get symbols array
        symbols = article.get('symbols', [])
        
        # Skip articles with empty symbols array (Requirement 2.1)
        if not symbols:
            logger.debug(f"Skipping article with empty symbols: article_id={article_id}")
            return tasks
        
        # Define the three components to process (Requirement 2.4)
        components = ['content', 'headline', 'summary']
        
        # Create separate task for each symbol-component combination (Requirement 2.2)
        for symbol in symbols:
            for component in components:
                # Get component text, using None for empty/null fields (Requirement 2.3)
                text = article.get(component)
                
                # Convert empty strings to None
                if text == '':
                    text = None
                
                if text is None:
                    logger.debug(
                        f"Empty component field: article_id={article_id}, "
                        f"symbol={symbol}, component={component}"
                    )
                
                # Create analysis task
                task = AnalysisTask(
                    article_id=article_id,
                    symbol=symbol,
                    component=component,
                    text=text,
                    created_at=created_at
                )
                tasks.append(task)
        
        logger.debug(
            f"Created {len(tasks)} tasks for article: article_id={article_id}, "
            f"symbols={len(symbols)}"
        )
        
        return tasks
