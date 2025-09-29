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


class WebScraperUtils:
    """
    This class handles all web scraping utility operations.
    """

    @staticmethod
    def remove_links_images_ui_junk(text: str) -> str:
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
    
   