import re
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Optional, List

import scrapy
from scrapy import Request
from scrapy.http import Response

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError, ConnectionLost
from twisted.web._newclient import ResponseNeverReceived


RECIPE_URL_RE = re.compile(r"^https://cookpad\.com/vn/cong-thuc/\d+")
RECIPE_URL_FMT = "https://cookpad.com/vn/cong-thuc/{rid}"


@dataclass(frozen=True)
class ScanMeta:
    rid: int
    source_url: str


class CookpadIdScanSpider(scrapy.Spider):
    name = "cookpad_id_scan"
    allowed_domains = ["cookpad.com"]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "RETRY_TIMES": 5,
        # Coverage tối đa: nhận mọi status code để tự xử lý trong parse
        "HTTPERROR_ALLOW_ALL": True,
        # RedirectMiddleware mặc định bật
    }

    def __init__(self, id_from: Optional[str] = None, id_to: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id_from = self._parse_int_arg("id_from", id_from)
        self.id_to = self._parse_int_arg("id_to", id_to)

        if self.id_from is None or self.id_to is None:
            raise ValueError("Cần truyền -a id_from=... -a id_to=...")

        if self.id_from > self.id_to:
            raise ValueError("id_from phải <= id_to")

    # ---------------------------
    # Scrapy entrypoints
    # ---------------------------

    def start_requests(self):
        for rid in range(self.id_from, self.id_to + 1):
            source_url = self._rid_to_url(rid)
            meta = ScanMeta(rid=rid, source_url=source_url)

            yield Request(
                url=source_url,
                callback=self.parse_recipe,
                errback=self.on_request_error,
                dont_filter=True,
                meta={"scan": meta},
            )

    def parse_recipe(self, response: Response):
        scan: ScanMeta = response.meta["scan"]
        source_url = scan.source_url
        final_url = response.url
        status = response.status
        redirected = (final_url != source_url)

        # Base fields (luôn có)
        base = self._base_item(
            scan=scan,
            final_url=final_url,
            http_status=status,
            redirected=redirected,
        )

        # 1) HTTP không OK -> ghi nhận và đi tiếp
        if status != 200:
            yield self._with_status(
                base,
                extract_status=self._status_from_http(status),
                extract_error=f"http_{status}",
                fingerprint_seed=f"{source_url}|{final_url}|http_{status}",
            )
            return

        # 2) Redirect sang non-recipe URL -> ghi nhận
        if not self._is_recipe_url(final_url):
            yield self._with_status(
                base,
                extract_status="redirected_non_recipe",
                extract_error="redirected_to_non_recipe",
                fingerprint_seed=f"{source_url}|{final_url}|redirected_non_recipe",
            )
            return

        # 3) Guard chống blocked/unexpected (nhẹ, không quá aggressive)
        # Bạn có thể tinh chỉnh condition khi đã quan sát HTML thực tế.
        page_title = (response.css("title::text").get() or "").strip()
        if not page_title:
            yield self._with_status(
                base,
                extract_status="blocked_or_unexpected",
                extract_error="empty_title",
                fingerprint_seed=f"{source_url}|{final_url}|empty_title",
            )
            return

        # 4) TODO: extract nội dung thật (name/image/ingredients/instructions)
        # Hiện yield tối thiểu để staging chạy trơn:
        item = {
            **base,
            "page_title": page_title,
            "name": None,
            "image_url": None,
            "ingredients_raw": [],
            "instructions_raw": [],
        }

        yield self._with_status(
            item,
            extract_status="ok",
            extract_error=None,
            fingerprint_seed=self._fingerprint_payload(
                source_url=source_url,
                final_url=final_url,
                http_status=status,
                name=item["name"],
                image_url=item["image_url"],
                ingredients=item["ingredients_raw"],
                instructions=item["instructions_raw"],
            ),
            already_hashed=True,
        )

    def on_request_error(self, failure):
        request = failure.request
        scan: ScanMeta = request.meta.get("scan")
        source_url = scan.source_url if scan else request.url
        rid = scan.rid if scan else None

        err_code = self._classify_failure(failure)

        # final_url chưa có response -> request.url
        item = {
            "source_url": source_url,
            "rid": rid,
            "final_url": request.url,
            "http_status": None,
            "was_redirected": False,
            "extract_status": "request_failed",
            "extract_error": err_code,
            "content_fingerprint": self._hash(f"{source_url}|{request.url}|{err_code}"),
        }
        yield item

    # ---------------------------
    # Builders / helpers
    # ---------------------------

    @staticmethod
    def _parse_int_arg(name: str, value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            return int(value)
        except ValueError as e:
            raise ValueError(f"Tham số {name} phải là số nguyên") from e

    @staticmethod
    def _rid_to_url(rid: int) -> str:
        return RECIPE_URL_FMT.format(rid=rid)

    @staticmethod
    def _is_recipe_url(url: str) -> bool:
        return bool(RECIPE_URL_RE.search(url))

    @staticmethod
    def _status_from_http(code: int) -> str:
        if code == 404:
            return "not_found"
        if code == 410:
            return "gone"
        if code == 429:
            return "rate_limited"
        if 500 <= code <= 599:
            return "server_error"
        return f"http_{code}"

    def _base_item(self, scan: ScanMeta, final_url: str, http_status: Optional[int], redirected: bool) -> Dict[str, Any]:
        return {
            "source_url": scan.source_url,
            "rid": scan.rid,
            "final_url": final_url,
            "http_status": http_status,
            "was_redirected": redirected,
        }

    def _with_status(
        self,
        item: Dict[str, Any],
        extract_status: str,
        extract_error: Optional[str],
        fingerprint_seed: str,
        already_hashed: bool = False,
    ) -> Dict[str, Any]:
        fp = fingerprint_seed if already_hashed else self._hash(fingerprint_seed)
        return {
            **item,
            "extract_status": extract_status,
            "extract_error": extract_error,
            "content_fingerprint": fp,
        }

    @staticmethod
    def _hash(s: str) -> str:
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    def _fingerprint_payload(
        self,
        source_url: str,
        final_url: str,
        http_status: int,
        name: Optional[str],
        image_url: Optional[str],
        ingredients: List[Any],
        instructions: List[Any],
    ) -> str:
        # Fingerprint ổn định hơn “source|final|status”
        # (ETL sau có thể recompute lại nếu bạn muốn)
        payload = (
            f"source={source_url}|final={final_url}|status={http_status}|"
            f"name={name or ''}|img={image_url or ''}|"
            f"ing={len(ingredients)}|ins={len(instructions)}"
        )
        return self._hash(payload)

    @staticmethod
    def _classify_failure(failure) -> str:
        # HttpError: xảy ra khi HttpErrorMiddleware xử lý một số trường hợp đặc biệt
        if failure.check(HttpError):
            response = failure.value.response
            return f"http_{getattr(response, 'status', 'unknown')}"

        if failure.check(DNSLookupError):
            return "dns_error"

        if failure.check(TimeoutError, TCPTimedOutError):
            return "timeout"

        if failure.check(ResponseNeverReceived, ConnectionLost):
            return "connection_error"

        return f"request_error:{failure.value.__class__.__name__}"
