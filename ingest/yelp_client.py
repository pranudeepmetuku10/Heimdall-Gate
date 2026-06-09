"""Thin Yelp Fusion client.

Scope: just the `/businesses/search` endpoint, paged. We deliberately avoid the
`/businesses/{id}/reviews` endpoint because Yelp's ToU restrictions around
review content are stricter than around business metadata.

Behavior worth knowing:

  - All requests carry the Bearer token; we never log it.
  - Retries with exponential backoff on 5xx and 429. 4xx other than 429 is
    raised immediately - retrying a 400 will not fix it.
  - Yelp returns `total` and a `businesses` array. We cap paging at
    `max_pages` regardless of `total`. The 1000-result cap on Yelp's side means
    pages beyond offset=1000 return empty anyway.
  - The client is process-local and not thread-safe (httpx.Client is, but our
    counter is not). The ingestion runner uses one client serially.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

log = logging.getLogger(__name__)


class YelpAPIError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"yelp_api_error status={status} body={body[:200]}")
        self.status = status
        self.body = body


class YelpRetryableError(YelpAPIError):
    """Subclass that retry logic should retry on (5xx, 429)."""


class _BudgetExceeded(RuntimeError):
    pass


class YelpClient:
    """Synchronous Yelp Fusion API client.

    The `api_call_budget` is a hard ceiling enforced in-process; once exceeded
    further calls raise. This is a guardrail against runaway loops eating the
    5,000/day Yelp quota during development.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.yelp.com/v3",
        timeout: float = 10.0,
        api_call_budget: int = 2000,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key required")
        self._client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Accept": "application/json",
                "User-Agent": "heimdall-gate/0.1 (+https://github.com/local)",
            },
            transport=transport,
        )
        self._api_call_budget = api_call_budget
        self._calls_made = 0

    @property
    def calls_made(self) -> int:
        return self._calls_made

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "YelpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- public --------------------------------------------------------------

    def search_businesses(
        self,
        *,
        location: str,
        term: str = "restaurants",
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """One page of the search endpoint."""
        return self._get(
            "/businesses/search",
            params={
                "location": location,
                "term": term,
                "limit": limit,
                "offset": offset,
            },
        )

    def iter_search(
        self,
        *,
        location: str,
        term: str,
        page_size: int,
        max_pages: int,
    ) -> Iterator[dict[str, Any]]:
        """Yield each business across up to `max_pages` of search results.

        Stops early when:
          - Yelp returns no businesses for a page
          - we hit the per-process call budget
        """
        for page in range(max_pages):
            offset = page * page_size
            if offset >= 1000:
                # Yelp's hard cap; further pages return empty.
                break
            try:
                body = self.search_businesses(
                    location=location,
                    term=term,
                    limit=page_size,
                    offset=offset,
                )
            except _BudgetExceeded:
                log.warning(
                    "yelp.budget_exceeded",
                    extra={"calls_made": self._calls_made},
                )
                return
            businesses = body.get("businesses") or []
            if not businesses:
                return
            for biz in businesses:
                yield biz
            if len(businesses) < page_size:
                return

    # -- private -------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type(YelpRetryableError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def _get(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        if self._calls_made >= self._api_call_budget:
            raise _BudgetExceeded()
        self._calls_made += 1

        try:
            resp = self._client.get(path, params=params)
        except httpx.TransportError as exc:
            raise YelpRetryableError(0, str(exc)) from exc

        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            raise YelpRetryableError(resp.status_code, resp.text)
        if resp.status_code >= 400:
            raise YelpAPIError(resp.status_code, resp.text)
        return resp.json()
