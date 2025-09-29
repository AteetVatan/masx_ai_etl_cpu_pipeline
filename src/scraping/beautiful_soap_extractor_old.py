# ┌───────────────────────────────────────────────────────────────┐
# │  Copyright (c) 2025 Ateet Vatan Bahmani                       │
# │  Project: MASX AI – Strategic Agentic AI System               │
# │  All rights reserved.                                         │
# └───────────────────────────────────────────────────────────────┘
#
# MASX AI is a proprietary software system developed and owned by Ateet Vatan Bahmani.
# The source code, documentation, workflows, designs, and naming (including "MASX AI")
# are protected by applicable copyright and trademark laws.
#
# Redistribution, modification, commercial use, or publication of any portion of this
# project without explicit written consent is strictly prohibited.
#
# This project is not open-source and is intended solely for internal, research,
# or demonstration use by the author.
#
# Contact: ab@masxai.com | MASXAI.com

"""
This module handles all BeautifulSoup-related operations in the MASX AI News ETL pipeline.
"""

import html
from random import choice
from typing import Dict, Any, Optional, List
from datetime import datetime
import re
from urllib.parse import urljoin


import requests
from bs4 import BeautifulSoup, Comment
from bs4.element import NavigableString, Tag


class BeautifulSoupExtractor_old:
    """
    This class handles all BeautifulSoup-related operations in the MASX AI News ETL pipeline.
    """

    @staticmethod
    def beautiful_soup_scrape(url, proxy):
        """
        Scrape the article using BeautifulSoup.
        """
        try:
            headers = {
                "User-Agent": proxy if isinstance(proxy, str) else choice(list(proxy))
            }
            # url = "https://baijiahao.baidu.com/s?id=1834495451984756877"
            response = requests.get(
                url, headers=headers, timeout=3600
            )  # 1 hour for complex pages
            response.encoding = response.apparent_encoding
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            print(f"⚠️ Failed to scrape {url}: {e}")
            return None

    @staticmethod
    def extract_text_from_soup(soup):
        """
        Extract the text from the BeautifulSoup object.
        """
        try:
            # title_tag = soup.find('title')
            # title = title_tag.get_text(strip=True) if title_tag else "Untitled"
            paragraphs = soup.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in paragraphs)
            text = html.unescape(text.replace("\xa0", " "))
            return text
        except Exception as e:
            print(f"Failed to extract text from {soup}: {e}")
            return None
        
    @staticmethod   
    def extract_article_data(soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """
        Extract article data from parsed HTML.
        
        Args:
            soup: BeautifulSoup parsed HTML
            url: Original URL for reference
            
        Returns:
            Dictionary with extracted article data
        """
        # Remove unwanted elements
        soup = BeautifulSoupExtractor_old.clean_soup(soup)
        
        # Extract title
        title = BeautifulSoupExtractor_old.extract_title(soup)
        
        # Extract author
        author = BeautifulSoupExtractor_old.extract_author(soup)
        
        # Extract publication date
        published_date = BeautifulSoupExtractor_old.extract_published_date(soup)
        
        # Extract main content
        content = BeautifulSoupExtractor_old.extract_content(soup)
        
        # Extract images
        images = BeautifulSoupExtractor_old.extract_images(soup, url)
        
        # Extract metadata
        metadata = BeautifulSoupExtractor_old.extract_metadata(soup)
        
        return {
            "url": url,
            "title": title,
            "author": author,
            "published_date": published_date,
            "content": content,
            "images": images,
            "metadata": metadata,
            "scraped_at": datetime.utcnow().isoformat(),
            "word_count": len(content.split()) if content else 0
        }
    
    @staticmethod
    def clean_soup(soup: BeautifulSoup) -> BeautifulSoup:
        """Remove unwanted elements from the soup."""
        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer", "aside"]):
            element.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Remove elements with common unwanted classes
        unwanted_classes = [
            "advertisement", "ad", "ads", "sidebar", "menu", "navigation",
            "social", "share", "comments", "related", "recommended"
        ]
        
        for class_name in unwanted_classes:
            for element in soup.find_all(class_=re.compile(class_name, re.I)):
                element.decompose()
                
        return soup
    
    
    @staticmethod
    def extract_title(soup: BeautifulSoup) -> Optional[str]:
        """Extract article title."""
        for selector in BeautifulSoupExtractor_old.title_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                return element.get_text(strip=True)
        return None
    
    @staticmethod
    def extract_author(soup: BeautifulSoup) -> Optional[str]:
        """Extract article author."""
        for selector in BeautifulSoupExtractor_old.author_selectors:
            element = soup.select_one(selector)
            if element and element.get_text(strip=True):
                return element.get_text(strip=True)
        return None
    
    @staticmethod
    def extract_published_date(soup: BeautifulSoup) -> Optional[str]:
        """Extract publication date."""
        for selector in BeautifulSoupExtractor_old.date_selectors:
            element = soup.select_one(selector)
            if element:
                # Try to get datetime attribute first
                datetime_attr = element.get('datetime')
                if datetime_attr:
                    return datetime_attr
                
                # Fallback to text content
                text = element.get_text(strip=True)
                if text:
                    return text
        return None
    
    @staticmethod
    def extract_content(soup: BeautifulSoup) -> str:
        """Extract main article content."""
        # Try different article selectors
        for selector in BeautifulSoupExtractor_old.article_selectors:
            article_element = soup.select_one(selector)
            if article_element:
                content = BeautifulSoupExtractor_old.extract_text_from_element(article_element)
                if len(content.strip()) > 200:  # Minimum content length
                    return content
        
        # Fallback: try to find the largest text block
        paragraphs = soup.find_all('p')
        if paragraphs:
            content = ' '.join(p.get_text(strip=True) for p in paragraphs)
            if len(content.strip()) > 200:
                return content
        
        # Last resort: get all text
        return soup.get_text(separator=' ', strip=True)
    
    @staticmethod
    def extract_text_from_element(element: Tag) -> str:
        """Extract clean text from a specific element."""
        # Remove unwanted child elements
        for unwanted in element.find_all(['script', 'style', 'nav', 'aside']):
            unwanted.decompose()
        
        # Extract text with proper spacing
        text_parts = []
        for child in element.descendants:
            if isinstance(child, NavigableString):
                text = child.strip()
                if text:
                    text_parts.append(text)
            elif child.name in ['p', 'div', 'br']:
                text_parts.append(' ')
        
        return ' '.join(text_parts)
    
    @staticmethod
    def extract_images(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
        """Extract images from the article."""
        images = []
        img_elements = soup.find_all('img')
        
        for img in img_elements:
            src = img.get('src')
            if not src:
                continue
            
            # Convert relative URLs to absolute
            if src.startswith('//'):
                src = 'https:' + src
            elif src.startswith('/'):
                src = urljoin(base_url, src)
            elif not src.startswith('http'):
                src = urljoin(base_url, src)
            
            # Extract alt text and other attributes
            alt_text = img.get('alt', '')
            title = img.get('title', '')
            
            images.append({
                "url": src,
                "alt": alt_text,
                "title": title,
                "width": img.get('width'),
                "height": img.get('height')
            })
        
        return images
    
    @staticmethod
    def extract_metadata(soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract additional metadata from the page."""
        metadata = {}
        
        # Extract meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                metadata[name] = content
        
        # Extract Open Graph tags
        og_tags = soup.find_all('meta', property=re.compile(r'^og:'))
        for tag in og_tags:
            property_name = tag.get('property')
            content = tag.get('content')
            if property_name and content:
                metadata[property_name] = content
        
        return metadata    
