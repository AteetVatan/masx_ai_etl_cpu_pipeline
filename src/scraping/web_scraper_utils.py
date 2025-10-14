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
This module handles all web scraping utility operations.
"""

import re
from urllib.parse import urljoin
from typing import List
from .error_patterns import ERROR_REGEX

class WebScraperUtils:
    """
    This class handles all web scraping utility operations.
    """
    
    @staticmethod
    def extract_image_urls(text: str) -> List[str]:
        """
        Extract image URLs (Markdown, HTML, or plain URLs) from text.

        Returns:
            A list of image URLs found in the text.
        """
        if not text:
            return []

        urls = set()

        # Markdown images: ![alt](url)
        urls.update(re.findall(r'!\[.*?\]\((https?://[^\s)]+)\)', text))

        # HTML image tags: <img src="url">
        urls.update(re.findall(r'<img[^>]+src=["\'](https?://[^"\']+)["\']', text))

        # Direct image links (ending in jpg/png/gif/webp/jpeg)
        urls.update(re.findall(r'(https?://[^\s]+?\.(?:jpg|jpeg|png|gif|webp))', text, re.IGNORECASE))

        # Relative or local paths (optional, depending on your input data)
        urls.update(re.findall(r'src=["\']([^"\']+\.(?:jpg|jpeg|png|gif|webp))["\']', text, re.IGNORECASE))

        return list(urls)

    @staticmethod
    def remove_ui_junk(text: str) -> str:
        # Step 0: Normalize invisible characters
        text = text.replace("\u200b", "")  # Zero-width space

        # Step 1: Remove Markdown image tags ![alt](url)
        text = re.sub(r"!\[.*?\]\(.*?\)", "", text)

        # Step 2: Convert [text](url) → text
        text = re.sub(r"\[([^\[\]]+)\]\([^\(\)]+\)", r"\1", text)

        # Step 3: Remove raw URLs
        text = re.sub(r"http[s]?://\S+", "", text)

        # Step 4: Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # Step 5: Remove markdown-style headers and rules
        text = re.sub(r"^#{1,6}\s+.*", "", text, flags=re.MULTILINE)
        text = re.sub(r"^(-{3,}|\*{3,})$", "", text, flags=re.MULTILINE)

        # Step 6: Remove fenced code blocks
        text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)

        # Step 7 (Optional): Remove emails and long numbers (potential phone numbers)
        text = re.sub(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", "", text)  # Emails
        text = re.sub(r"\b\d{10,}\b", "", text)  # Long numbers (10+ digits)

        # Step 8: Normalize and clean lines
        lines = text.splitlines()
        cleaned_lines = [
            re.sub(r"\s+", " ", line.strip()) for line in lines if line.strip()
        ]

        # Step 9: Remove duplicate empty lines
        text = "\n".join(cleaned_lines)
        text = re.sub(r"\n{2,}", "\n", text)

        return text.strip()
    
    
    @staticmethod
    def find_error_pattern(text: str) -> bool:
        """
        Detect if the given text contains any known connection / network / timeout error patterns.

        Args:
            text (str): The HTML, log, or plain text content to analyze.

        Returns:
            bool: True if an error pattern is found, False otherwise.
        """
        if not text:
            return False

        # Normalize for consistent matching
        text = text.lower()

        # Search for any pattern match
        return bool(ERROR_REGEX.search(text))