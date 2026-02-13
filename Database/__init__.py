"""
Database module for TrendingUpdate project.
Contains database operations and external API integrations.
"""

from . import serper_search
from . import news_db

__all__ = ['serper_search', 'news_db']
