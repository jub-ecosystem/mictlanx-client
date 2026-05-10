from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from mictlanx.interfaces.responses import PeerStatsResponse
from mictlanx.services import AsyncRouter


@dataclass
class StoragePool:
    """
    Represents a Virtual Storage Space (VSS): one AsyncRouter fronting a pool of
    storage peers.  Used by HEAPaCStrategy to select where to place new replicas
    and to monitor aggregate disk utilization and health.
    """
    pool_id: str
    router: AsyncRouter
    peer_ids: List[str] = field(default_factory=list)

    async def pool_stats(self) -> Optional[Dict[str, PeerStatsResponse]]:
        """Return per-peer stats from the router, or None if the router is unreachable."""
        result = await self.router.get_stats()
        return result.unwrap() if result.is_ok else None

    async def disk_utilization(self) -> float:
        """Average disk utilization (0.0–1.0) across all peers in this pool."""
        stats = await self.pool_stats()
        if not stats:
            return 0.0
        values = [s.disk_uf for s in stats.values()]
        return sum(values) / len(values) if values else 0.0

    async def available_disk_bytes(self) -> int:
        """Sum of available bytes across all peers in this pool."""
        stats = await self.pool_stats()
        if not stats:
            return 0
        return sum(s.available_disk for s in stats.values())

    async def is_healthy(self) -> bool:
        """True if the router is reachable and returning stats."""
        stats = await self.pool_stats()
        return stats is not None
