import scrapy
from urllib.parse import quote

from cookpad_ingest.utils import clean_lines, fingerprint


class CookpadVnSpider(scrapy.Spider):
    name = "cookpad_vn"
    allowed_domains = ["cookpad.com"]

    keywords = [
        "món việt",
        "món ngon",
        "canh",
        "kho",
        "xào",
        "chiên",
        "nướng",
        "bún",
        "phở",
        "cơm",
        "gà",
        "thịt heo",
        "bò",
        "cá",
        "tôm",
        "mực",
        "đậu hũ",
        "rau muống",
        "cà tím",
        "trứng",
        "cháo",
        "lẩu",
        "bánh",
        "chè",
        "gỏi",
        "salad",
    ]
    max_pages_per_keyword = 50

    def start_requests(self):
        for kw in self.keywords:
            for page in range(1, self.max_pages_per_keyword + 1):
                url = f"https://cookpad.com/vn/tim-kiem/{quote(kw)}?page={page}"
                yield scrapy.Request(url, callback=self.parse_search, meta={"kw": kw, "page": page})

    def parse_search(self, response):
        links = response.css('a[href^="/vn/cong-thuc/"]::attr(href)').getall()
        links = list(dict.fromkeys(links))
        if not links:
            self.logger.info(
                "No links kw=%s page=%s", response.meta.get("kw"), response.meta.get("page")
            )
            return
        for href in links:
            yield response.follow(href, callback=self.parse_recipe)

    def parse_recipe(self, response):
        out = {"source_url": response.url}
        try:
            name = response.css("h1::text").get() or response.css(
                'meta[property="og:title"]::attr(content)'
            ).get()
            name = (name or "").strip()
            out["name"] = name

            image_url = response.css('meta[property="og:image"]::attr(content)').get()
            out["image_url"] = image_url

            # Heuristic: fallback to generic list items; Cookpad layout may change
            ingredients = response.css("li::text").getall()
            ingredients = clean_lines(ingredients)[:120]

            instructions = response.css("ol li::text, .steps li::text, li::text").getall()
            instructions = clean_lines(instructions)[:120]

            out["ingredients_raw"] = ingredients
            out["instructions_raw"] = instructions

            out["content_fingerprint"] = fingerprint(name, ingredients, instructions)
            out["extract_status"] = "ok"
            out["extract_error"] = None
        except Exception as e:
            out["content_fingerprint"] = fingerprint(out.get("name", ""), [], [])
            out["extract_status"] = "fail"
            out["extract_error"] = str(e)

        yield out
