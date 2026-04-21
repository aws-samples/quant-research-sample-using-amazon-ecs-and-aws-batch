"""Tests for ArticleProcessor functionality."""

import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings
from article_processor import ArticleProcessor, AnalysisTask


class TestArticleProcessorPropertyTests:
    """Property-based tests for ArticleProcessor."""
    
    @given(
        num_articles=st.integers(min_value=1, max_value=20),
        data=st.data()
    )
    @settings(max_examples=100, deadline=None)
    def test_required_field_preservation_property(self, num_articles, data):
        """
        **Feature: parallel-s3-dataframe-optimization, Property 5: Required field preservation**
        
        For any article in the source S3 data, all fields required for sentiment analysis 
        (id, symbols, content, headline, summary) should be present in the corresponding DataFrame row.
        
        **Validates: Requirements 2.5**
        """
        processor = ArticleProcessor()
        
        # Generate articles with unique IDs and unique symbols per article
        articles = []
        for i in range(num_articles):
            # Generate unique symbols for this article
            num_symbols = data.draw(st.integers(min_value=1, max_value=5))
            symbols = [f"SYM{i}_{j}" for j in range(num_symbols)]
            
            article = {
                'id': f'article_{i}',
                'symbols': symbols,
                'content': data.draw(st.one_of(st.none(), st.text(min_size=0, max_size=100))),
                'headline': data.draw(st.one_of(st.none(), st.text(min_size=0, max_size=100))),
                'summary': data.draw(st.one_of(st.none(), st.text(min_size=0, max_size=100)))
            }
            articles.append(article)
        
        # Create DataFrame from articles
        df = pd.DataFrame(articles)
        
        # Process DataFrame
        tasks = processor.process_dataframe(df)
        
        # Verify all required fields are preserved
        # Each article should generate tasks with all required fields
        for article in articles:
            article_id = str(article['id'])
            symbols = article['symbols']
            
            # Find all tasks for this article
            article_tasks = [t for t in tasks if t.article_id == article_id]
            
            # Should have tasks for each symbol-component combination
            expected_task_count = len(symbols) * 3  # 3 components per symbol
            assert len(article_tasks) == expected_task_count, \
                f"Expected {expected_task_count} tasks for article {article_id}, got {len(article_tasks)}"
            
            # Verify each component is represented
            for symbol in symbols:
                for component in ['content', 'headline', 'summary']:
                    # Find task for this symbol-component combination
                    matching_tasks = [
                        t for t in article_tasks 
                        if t.symbol == symbol and t.component == component
                    ]
                    
                    assert len(matching_tasks) == 1, \
                        f"Expected exactly 1 task for {article_id}/{symbol}/{component}, got {len(matching_tasks)}"
                    
                    task = matching_tasks[0]
                    
                    # Verify the text field matches the article's component value
                    expected_text = article[component]
                    if expected_text == '':
                        expected_text = None
                    
                    assert task.text == expected_text, \
                        f"Task text mismatch for {article_id}/{symbol}/{component}: expected {expected_text}, got {task.text}"


class TestArticleProcessorUnitTests:
    """Unit tests for ArticleProcessor."""
    
    def test_dataframe_processing_basic(self):
        """Test basic DataFrame processing with valid articles."""
        processor = ArticleProcessor()
        
        # Create test DataFrame
        articles = [
            {
                'id': 'article1',
                'symbols': ['AAPL', 'GOOGL'],
                'content': 'Test content',
                'headline': 'Test headline',
                'summary': 'Test summary'
            },
            {
                'id': 'article2',
                'symbols': ['MSFT'],
                'content': 'Another content',
                'headline': None,
                'summary': 'Another summary'
            }
        ]
        df = pd.DataFrame(articles)
        
        # Process DataFrame
        tasks = processor.process_dataframe(df)
        
        # Verify task count: article1 has 2 symbols * 3 components = 6 tasks
        #                     article2 has 1 symbol * 3 components = 3 tasks
        assert len(tasks) == 9
        
        # Verify article1 tasks
        article1_tasks = [t for t in tasks if t.article_id == 'article1']
        assert len(article1_tasks) == 6
        
        # Verify article2 tasks
        article2_tasks = [t for t in tasks if t.article_id == 'article2']
        assert len(article2_tasks) == 3
    
    def test_backward_compatibility_with_iterator(self):
        """Test that Iterator input still works correctly."""
        processor = ArticleProcessor()
        
        # Create test articles as iterator
        articles = iter([
            {
                'id': 'article1',
                'symbols': ['AAPL'],
                'content': 'Test content',
                'headline': 'Test headline',
                'summary': 'Test summary'
            }
        ])
        
        # Process articles using existing method
        tasks = list(processor.process_articles(articles))
        
        # Verify task count: 1 article * 1 symbol * 3 components = 3 tasks
        assert len(tasks) == 3
    
    def test_max_articles_limiting(self):
        """Test max_articles parameter limits processing."""
        processor = ArticleProcessor()
        
        # Create test DataFrame with 5 articles
        articles = [
            {
                'id': f'article{i}',
                'symbols': ['AAPL'],
                'content': f'Content {i}',
                'headline': f'Headline {i}',
                'summary': f'Summary {i}'
            }
            for i in range(5)
        ]
        df = pd.DataFrame(articles)
        
        # Process with max_articles=2
        tasks = processor.process_dataframe(df, max_articles=2)
        
        # Should only process 2 articles: 2 * 1 symbol * 3 components = 6 tasks
        assert len(tasks) == 6
        
        # Verify only first 2 articles were processed
        article_ids = set(t.article_id for t in tasks)
        assert article_ids == {'article0', 'article1'}
    
    def test_empty_symbols_filtering(self):
        """Test that articles with empty symbols are filtered out."""
        processor = ArticleProcessor()
        
        # Create test DataFrame with empty symbols
        articles = [
            {
                'id': 'article1',
                'symbols': ['AAPL'],
                'content': 'Test content',
                'headline': 'Test headline',
                'summary': 'Test summary'
            },
            {
                'id': 'article2',
                'symbols': [],  # Empty symbols
                'content': 'Another content',
                'headline': 'Another headline',
                'summary': 'Another summary'
            }
        ]
        df = pd.DataFrame(articles)
        
        # Process DataFrame
        tasks = processor.process_dataframe(df)
        
        # Should only have tasks for article1: 1 * 1 symbol * 3 components = 3 tasks
        assert len(tasks) == 3
        assert all(t.article_id == 'article1' for t in tasks)
    
    def test_various_dataframe_structures(self):
        """Test task creation from various DataFrame structures."""
        processor = ArticleProcessor()
        
        # Test with None values
        articles = [
            {
                'id': 'article1',
                'symbols': ['AAPL'],
                'content': None,
                'headline': 'Test headline',
                'summary': None
            }
        ]
        df = pd.DataFrame(articles)
        tasks = processor.process_dataframe(df)
        
        assert len(tasks) == 3
        content_task = [t for t in tasks if t.component == 'content'][0]
        assert content_task.text is None
        
        # Test with empty strings
        articles = [
            {
                'id': 'article2',
                'symbols': ['GOOGL'],
                'content': '',
                'headline': 'Test headline',
                'summary': ''
            }
        ]
        df = pd.DataFrame(articles)
        tasks = processor.process_dataframe(df)
        
        assert len(tasks) == 3
        content_task = [t for t in tasks if t.component == 'content'][0]
        assert content_task.text is None  # Empty strings converted to None
        
        # Test with multiple symbols
        articles = [
            {
                'id': 'article3',
                'symbols': ['AAPL', 'GOOGL', 'MSFT'],
                'content': 'Test content',
                'headline': 'Test headline',
                'summary': 'Test summary'
            }
        ]
        df = pd.DataFrame(articles)
        tasks = processor.process_dataframe(df)
        
        # 3 symbols * 3 components = 9 tasks
        assert len(tasks) == 9
        symbols_in_tasks = set(t.symbol for t in tasks)
        assert symbols_in_tasks == {'AAPL', 'GOOGL', 'MSFT'}
