"""A minimal client for the Toxiproxy HTTP API (https://github.com/Shopify/toxiproxy).

Only what the fault schedule uses: create a proxy, enable/disable it (a partition), and
add/remove one named toxic. Every call raises on a non-2xx reply, so a fault that didn't
happen can never be counted as one.
"""

from typing import Any

import httpx

TOXIC = "chaos"  # the one toxic a fault adds to a proxy at a time


class Toxiproxy:
    def __init__(self, base_url: str) -> None:
        self._http = httpx.AsyncClient(base_url=base_url, timeout=10)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _call(self, method: str, path: str, body: Any = None) -> Any:
        resp = await self._http.request(method, path, json=body)
        resp.raise_for_status()
        return resp.json() if resp.content else None

    async def ping(self) -> None:
        await self._call("GET", "/version")

    async def reset(self) -> None:
        """Enable every proxy and remove every toxic (heal everything)."""
        await self._call("POST", "/reset")

    async def create_proxy(self, name: str, listen: str, upstream: str) -> None:
        await self._call("POST", "/proxies", {"name": name, "listen": listen, "upstream": upstream})

    async def set_enabled(self, name: str, enabled: bool) -> None:
        """Disabled = partitioned: open connections are closed and new ones refused."""
        await self._call("POST", f"/proxies/{name}", {"enabled": enabled})

    async def add_toxic(self, proxy: str, kind: str, attributes: dict[str, int]) -> None:
        """Add the TOXIC toxic to `proxy`, on the downstream (Redis -> worker) side.

        Downstream matters for `timeout`: the worker's commands still reach Redis and run,
        but the replies are dropped. That is the "lost reply" case every script must
        survive by being safe to re-send (ADR-006).
        """
        await self._call(
            "POST",
            f"/proxies/{proxy}/toxics",
            {
                "name": TOXIC,
                "type": kind,
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": attributes,
            },
        )

    async def remove_toxic(self, proxy: str) -> None:
        await self._call("DELETE", f"/proxies/{proxy}/toxics/{TOXIC}")
