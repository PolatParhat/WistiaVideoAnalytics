"""Wistia Stats API client: Bearer auth, retries with backoff, pagination."""

import logging
import random
import time

import requests

from wistia.config import BASE_URL, MAX_ATTEMPTS, PER_PAGE
from wistia.logs import get_logger, log_event

log = get_logger("wistia.api")

RETRY_STATUS = {429, 500, 502, 503, 504}


class WistiaApiError(RuntimeError):
    """Raised when a call fails for good (non-retryable status, or retries used up)."""


class WistiaClient:
    def __init__(self, token, base_url=BASE_URL, max_attempts=MAX_ATTEMPTS, per_page=PER_PAGE,
                 session=None, sleep=time.sleep):
        self.base_url = base_url
        self.max_attempts = max_attempts
        self.per_page = per_page
        self.session = session or requests.Session()
        # Bearer is Wistia's documented method (Basic also works; see docs/decisions.md)
        self.session.headers["Authorization"] = f"Bearer {token}"
        self._sleep = sleep  # injectable so unit tests don't really wait

    def url(self, path):
        return f"{self.base_url}{path}"

    def get_json(self, path, params=None):
        """GET one URL and return the parsed JSON. Retries temporary failures, raises on the rest."""
        url = self.url(path)
        for attempt in range(1, self.max_attempts + 1):
            started = time.monotonic()
            response = None
            try:
                response = self.session.get(url, params=params, timeout=30)
            except (requests.ConnectionError, requests.Timeout) as error:
                status, reason = None, type(error).__name__
            else:
                status, reason = response.status_code, response.reason
                fields = {"url": url, "params": params, "status": status, "attempt": attempt,
                          "ms": round((time.monotonic() - started) * 1000)}
                if status == 200:
                    log_event(log, "api call", **fields)
                    return response.json()
                if status not in RETRY_STATUS:
                    # 401 (bad token), 404 (wrong id) ... retrying will not help
                    log_event(log, "api call failed", logging.ERROR, **fields)
                    raise WistiaApiError(f"{status} {reason} for {url}")

            if attempt == self.max_attempts:
                raise WistiaApiError(
                    f"giving up on {url} after {self.max_attempts} attempts ({status} {reason})")
            wait = self._backoff(attempt, response)
            log_event(log, "retrying", logging.WARNING, url=url, status=status, reason=reason,
                      attempt=attempt, wait_s=round(wait, 1))
            self._sleep(wait)

    @staticmethod
    def _backoff(attempt, response):
        """Exponential backoff 1, 2, 4, 8 s plus jitter; honour Retry-After on 429."""
        if response is not None and response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "")
            if retry_after.isdigit():
                return float(retry_after)
        return 2 ** (attempt - 1) + random.random()

    def get_pages(self, path, params):
        """Yield (page_number, rows, params) for page 1, 2, ... until Wistia returns an empty list."""
        page = 1
        while True:
            page_params = {**params, "per_page": self.per_page, "page": page}
            rows = self.get_json(path, page_params)
            if not rows:
                return
            yield page, rows, page_params
            page += 1

    # ---- the three endpoints the pipeline uses ----
    def media(self, media_id):
        return self.get_json(f"/medias/{media_id}.json")

    def media_stats_by_date(self, media_id, start_date, end_date):
        params = {"start_date": start_date, "end_date": end_date}
        return self.get_json(f"/stats/medias/{media_id}/by_date.json", params), params

    def event_pages(self, media_id, start_date, end_date):
        params = {"media_id": media_id, "start_date": start_date, "end_date": end_date}
        return self.get_pages("/stats/events.json", params)
