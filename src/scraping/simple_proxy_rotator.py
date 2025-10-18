

import random
from typing import List
from crawl4ai.async_configs import ProxyConfig, ProxyRotationStrategy


class SimpleProxyRotator(ProxyRotationStrategy):
    def __init__(self, proxies: list[str]):
        # Normalize proxies to include protocol prefix
        proxies = [p if p.startswith("http") else f"http://{p}" for p in proxies]
        self.proxies = [ProxyConfig(server=p) for p in proxies]
        self.index = 0

    async def get_next_proxy(self) -> ProxyConfig:
        """Return the next proxy in rotation."""
        if not self.proxies:
            return None
        proxy = self.proxies[self.index % len(self.proxies)]
        self.index += 1
        return proxy

    def add_proxies(self, proxies: List[ProxyConfig]):
        """Add new proxies to the pool."""
        for p in proxies:
            if isinstance(p, str):
                p = ProxyConfig(server=p if p.startswith("http") else f"http://{p}")
            self.proxies.append(p)
