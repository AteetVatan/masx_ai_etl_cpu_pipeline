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
Proxy service for managing proxy operations.

This module provides a service class for interacting with the MASX AI proxy service,
including starting proxy refresh operations and retrieving available proxies.
"""

import aiohttp
import asyncio
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
import contextlib

from src.config import get_settings, get_service_logger
from src.core.exceptions import ServiceException


@dataclass
class ProxyStartResponse:
    """Response model for proxy start operation."""

    status: str
    duration: str
    timestamp: datetime = None

    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class ProxyListResponse:
    """Response model for proxy list operation."""

    success: bool
    data: List[str]
    message: str
    count: int = 0
    timestamp: datetime = None

    def __post_init__(self):
        """Set count and timestamp if not provided."""
        if self.count == 0:
            self.count = len(self.data) if self.data else 0
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ProxyService:
    """
    Singleton Service class for managing proxy operations with the MASX AI proxy service.

    This class provides methods to:
    1. Start proxy refresh operations
    2. Retrieve available proxy lists
    3. Handle authentication and error responses
    """

    _instance = None
    _lock: Lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ProxyService, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    @classmethod
    def get_instance(cls):
        return cls()

    def __init__(self):
        """Initialize the proxy service with configuration and logging."""
        if self._initialized:
            return

        self.settings = get_settings()
        self.logger = get_service_logger("ProxyService")
        self._refresher_task = None

        self._proxy_cache: List[str] = []
        self._proxy_cache_timestamp = None

        # Validate required configuration
        if not self.settings.proxy_api_key:
            self.logger.warning(
                "Proxy API key not configured - proxy operations may fail"
            )

        # Service endpoints
        self.base_url = self.settings.proxy_base
        self.start_endpoint = self.settings.proxy_post_start_service
        self.proxies_endpoint = self.settings.proxy_get_proxies

        # Headers for authentication
        self.headers = {
            "X-API-Key": self.settings.proxy_api_key,
            "Content-Type": "application/json",
            "User-Agent": "MASX-AI-ETL/1.0",
        }

        self.logger.info(f"ProxyService initialized with base URL: {self.base_url}")
        self._initialized = True

    async def get_proxy_cache(self, force_refresh: bool = False) -> List[str]:
        """Get the proxy cache, refreshing if expired."""
        if not self._proxy_cache or force_refresh:
            self._proxy_cache = await self.__get_proxies()           

        return self._proxy_cache 
    
    async def validate_proxies(self, proxies: List[str]) -> List[str]:
        """Validate the proxies from the proxy service."""
        import requests#
        valid_proxies = []
        for proxy in proxies:
            try:
                r = requests.get("https://httpbin.org/ip", proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"}, timeout=5)
                if r.status_code == 200:
                    valid_proxies.append(proxy)
            except Exception as e:
                #print(proxy, "FAILED:", e)
                pass
        self.logger.info(f"Validated {len(valid_proxies)} proxies out of {len(proxies)}")
        return valid_proxies
    

    async def _refresh(self):
        """Internal refresh method (safe)."""
        try:
            new_proxies = await self.__get_proxies()
            if new_proxies:
                self._proxy_cache = new_proxies
                self._proxy_cache_timestamp = datetime.now()
                self.logger.info("Proxy cache refreshed in background")
        except Exception as e:
            self.logger.error(f"Failed to refresh proxy cache: {e}")

    async def start_proxy_refresher(self, interval: int = 180):
        """Run a background loop to refresh proxies every `interval` seconds (default: 3 minutes)."""
        self.logger.info(f"Starting proxy refresher (every {interval//60} min)...")

        async def _loop():
            while True:
                await self._refresh()
                await asyncio.sleep(interval)

        # Fire and forget
        self._refresher_task = asyncio.create_task(_loop())

    async def stop_proxy_refresher(self):
        if hasattr(self, "_refresher_task") and self._refresher_task:
            self.logger.info("Stopping proxy refresher...")
            self._refresher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresher_task
            self.logger.info("Proxy refresher stopped.")

    async def ping_start_proxy(self) -> ProxyStartResponse:
        """
        Start proxy refresh operation by calling the proxy service.

        Returns:
            ProxyStartResponse: Response containing status and duration

        Raises:
            ServiceException: If the proxy start operation fails
        """
        try:
            self.logger.info("Starting proxy refresh operation...")

            # Validate configuration
            if not self.settings.proxy_api_key:
                raise ServiceException("Proxy API key not configured")

            # Prepare request
            url = f"{self.base_url}{self.start_endpoint}"
            self.logger.debug(f"Calling proxy start endpoint: {url}")

            # Make POST request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        self.logger.info("Proxy refresh operation started successfully")

                        # Parse response
                        proxy_response = ProxyStartResponse(
                            status=response_data.get("status", "unknown"),
                            duration=response_data.get("duration", "unknown"),
                        )

                        self.logger.info(
                            f"Proxy refresh status: {proxy_response.status}, "
                            f"Duration: {proxy_response.duration}"
                        )

                        await self.start_proxy_refresher(interval=180)

                        return proxy_response

                    elif response.status == 401:
                        error_msg = "Unauthorized - Invalid proxy API key"
                        self.logger.error(error_msg)
                        raise ServiceException(error_msg)

                    elif response.status == 429:
                        error_msg = "Rate limited - Too many requests to proxy service"
                        self.logger.warning(error_msg)
                        raise ServiceException(error_msg)

                    else:
                        error_msg = f"Proxy start failed with status {response.status}"
                        try:
                            error_data = await response.text()
                            error_msg += f": {error_data}"
                        except:
                            pass

                        self.logger.error(error_msg)
                        raise ServiceException(error_msg)

        except aiohttp.ClientError as e:
            error_msg = f"Network error during proxy start operation: {str(e)}"
            self.logger.error(error_msg)
            raise ServiceException(error_msg)

        except asyncio.TimeoutError:
            error_msg = "Timeout error during proxy start operation"
            self.logger.error(error_msg)
            raise ServiceException(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error during proxy start operation: {str(e)}"
            self.logger.error(error_msg)
            raise ServiceException(error_msg)

    async def __get_proxies(self) -> List[str]:
        """
        Retrieve list of available proxies from the proxy service.

        Returns:
            List[str]: List of proxy addresses in format "IP:PORT"

        Raises:
            ServiceException: If the proxy retrieval operation fails
        """
        try:
            self.logger.info("Retrieving proxy list...")

            # Validate configuration
            if not self.settings.proxy_api_key:
                raise ServiceException("Proxy API key not configured")

            # Prepare request
            url = f"{self.base_url}{self.proxies_endpoint}"
            self.logger.debug(f"Calling proxy list endpoint: {url}")

            # Make GET request
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=5 * 60),  # 5 minutes
                ) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        self.logger.info("Proxy list retrieved successfully")

                        # Parse response
                        proxy_response = ProxyListResponse(
                            success=response_data.get("success", False),
                            data=response_data.get("data", []),
                            message=response_data.get("message", ""),
                        )

                        if not proxy_response.success:
                            error_msg = f"Proxy service returned error: {proxy_response.message}"
                            self.logger.error(error_msg)
                            raise ServiceException(error_msg)
                        
                        

                        proxy_count = len(proxy_response.data)
                        self.logger.info(
                            f"Retrieved {proxy_count} proxies: {proxy_response.message}"
                        )

                        # Validate proxy data
                        
                        if not proxy_response.data:
                            self.logger.warning("No proxies returned from service")
                            return []
                        proxy_data = await self.validate_proxies(proxy_response.data)

                        # Cache proxies
                        self._proxy_cache = proxy_data

                        # Return proxy list
                        return proxy_data

                    elif response.status == 401:
                        error_msg = "Unauthorized - Invalid proxy API key"
                        self.logger.error(error_msg)
                        raise ServiceException(error_msg)

                    elif response.status == 429:
                        error_msg = "Rate limited - Too many requests to proxy service"
                        self.logger.warning(error_msg)
                        raise ServiceException(error_msg)

                    else:
                        error_msg = (
                            f"Proxy retrieval failed with status {response.status}"
                        )
                        try:
                            error_data = await response.text()
                            error_msg += f": {error_data}"
                        except:
                            pass

                        self.logger.error(error_msg)
                        raise ServiceException(error_msg)

        except aiohttp.ClientError as e:
            error_msg = f"Network error during proxy retrieval: {str(e)}"
            self.logger.error(error_msg)
            raise ServiceException(error_msg)

        except asyncio.TimeoutError:
            error_msg = "Timeout error during proxy retrieval"
            self.logger.error(error_msg)
            raise ServiceException(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error during proxy retrieval: {str(e)}"
            self.logger.error(error_msg)
            raise ServiceException(error_msg)

    async def health_check(self) -> bool:
        """
        Perform a health check on the proxy service.

        Returns:
            bool: True if service is healthy, False otherwise
        """
        try:
            self.logger.info("Performing proxy service health check...")

            # Try to get proxies as a health check
            proxies = await self.__get_proxies()

            if proxies and len(proxies) > 0:
                self.logger.info("Proxy service health check passed")
                return True
            else:
                self.logger.warning(
                    "Proxy service health check failed - no proxies returned"
                )
                return False

        except Exception as e:
            self.logger.error(f"Proxy service health check failed: {str(e)}")
            return False

    def get_proxy_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the proxy service configuration.

        Returns:
            Dict[str, Any]: Configuration statistics
        """
        return {
            "base_url": self.base_url,
            "start_endpoint": self.start_endpoint,
            "proxies_endpoint": self.proxies_endpoint,
            "api_key_configured": bool(self.settings.proxy_api_key),
            "headers_configured": bool(self.headers.get("X-API-Key")),
            "service_ready": bool(self.settings.proxy_api_key),
        }
