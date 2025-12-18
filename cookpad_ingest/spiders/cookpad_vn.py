# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import scrapy


@dataclass(frozen=True)
class ExtractResult:
    name: Optional[str]
    image_url: Optional[str]
    ingredients_raw: List[str]
    instructions_raw: List[str]
    servings_raw: Optional[str]
    cuisine: Optional[str]
    description: Optional[str]
    author_name: Optional[str]
    extract_source: str  # "jsonld" | "dom"
    extract_error: Optional[str]


class CookpadVnSpider(scrapy.Spider):
    """
    Clean, robust extractor for Cookpad recipe pages.

    Strategy:
      1) Parse JSON-LD Recipe (preferred)
      2) Fallback to DOM selectors (best-effort)
    """
    name = "cookpad_vn"
    allowed_domains = ["cookpad.com"]
    # TODO: Replace with your entry points (collection pages / search pages / seeds)
    start_urls = [
        # Example: "https://cookpad.com/vn/recipes/xxxxxx"
    ]

    custom_settings = {
        # Keep it gentle: you can tune later
        "DOWNLOAD_DELAY": 0.25,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.25,
        "AUTOTHROTTLE_MAX_DELAY": 5.0,
        "ROBOTSTXT_OBEY": True,
        # If you already have a pipeline/staging writer, keep your existing config
    }

    # -------------------------
    # Public entry
    # -------------------------
    def parse(self, response: scrapy.http.Response, **kwargs: Any):
        """
        If your start_urls contain recipe pages, parse directly.
        If they are listing pages, override and add link discovery here.
        """
        # If listing page, discover recipe links:
        # for href in response.css('a[href*="/vn/recipes/"]::attr(href)').getall():
        #     yield response.follow(href, callback=self.parse_recipe)

        yield from self.parse_recipe(response)

    def parse_recipe(self, response: scrapy.http.Response):
        out: Dict[str, Any] = {
            "source_url": response.url,
            "extract_status": "fail",
            "extract_error": None,
            "extract_source": None,
        }

        try:
            # 1) JSON-LD
            jsonld = self._extract_recipe_jsonld(response)
            if jsonld:
                res = self._parse_from_jsonld(jsonld)
            else:
                res = None

            # 2) DOM fallback
            if not res or (not res.ingredients_raw and not res.instructions_raw):
                res = self._parse_from_dom(response)

            # Validate minimal fields
            ingredients = self._clean_lines(res.ingredients_raw if res else [])
            instructions = self._clean_lines(res.instructions_raw if res else [])

            # Name fallback: page <title> if missing
            name = (res.name if res else None) or self._fallback_title(response)

            # Final validation
            if not name:
                raise ValueError("Missing recipe name")
            if not ingredients:
                raise ValueError("Missing ingredients")
            if not instructions:
                raise ValueError("Missing instructions")

            out.update({
                "name": name,
                "image_url": res.image_url if res else None,
                "servings_raw": res.servings_raw if res else None,
                "cuisine": res.cuisine if res else None,
                "description": res.description if res else None,
                "author_name": res.author_name if res else None,
                "ingredients_raw": ingredients,
                "instructions_raw": instructions,
                "content_fingerprint": self._fingerprint(name, ingredients, instructions),
                "extract_status": "ok",
                "extract_error": None,
                "extract_source": res.extract_source if res else None,
            })

        except Exception as e:
            out.update({
                "name": out.get("name"),
                "image_url": out.get("image_url"),
                "ingredients_raw": out.get("ingredients_raw", []) or [],
                "instructions_raw": out.get("instructions_raw", []) or [],
                "content_fingerprint": self._fingerprint(out.get("name", "") or "", [], []),
                "extract_status": "fail",
                "extract_error": str(e),
                "extract_source": out.get("extract_source"),
            })

        yield out

    # -------------------------
    # JSON-LD extraction
    # -------------------------
    def _extract_recipe_jsonld(self, response: scrapy.http.Response) -> Optional[Dict[str, Any]]:
        """
        Returns the first JSON-LD object that represents a Recipe.
        Handles:
          - dict with @type Recipe
          - list containing Recipe
          - dict with @graph containing Recipe
        """
        blocks = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        if not blocks:
            return None

        for raw in blocks:
            raw = raw.strip()
            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                continue

            recipe = self._find_recipe_in_jsonld(data)
            if recipe:
                return recipe

        return None

    def _find_recipe_in_jsonld(self, data: Any) -> Optional[Dict[str, Any]]:
        if isinstance(data, dict):
            # direct
            if self._is_recipe_type(data.get("@type")):
                return data
            # @graph
            graph = data.get("@graph")
            if isinstance(graph, list):
                for node in graph:
                    if isinstance(node, dict) and self._is_recipe_type(node.get("@type")):
                        return node
            return None

        if isinstance(data, list):
            for node in data:
                if isinstance(node, dict) and self._is_recipe_type(node.get("@type")):
                    return node
                # nested @graph inside list items
                if isinstance(node, dict) and isinstance(node.get("@graph"), list):
                    for g in node["@graph"]:
                        if isinstance(g, dict) and self._is_recipe_type(g.get("@type")):
                            return g
        return None

    def _is_recipe_type(self, t: Any) -> bool:
        if t == "Recipe":
            return True
        if isinstance(t, list) and "Recipe" in t:
            return True
        return False

    def _parse_from_jsonld(self, recipe: Dict[str, Any]) -> ExtractResult:
        name = recipe.get("name")
        image_url = self._coerce_image(recipe.get("image"))
        servings_raw = recipe.get("recipeYield")
        cuisine = recipe.get("recipeCuisine")
        description = recipe.get("description")

        # Ingredients
        ingredients = recipe.get("recipeIngredient") or []
        if isinstance(ingredients, str):
            ingredients = [ingredients]

        # Instructions: can be list of HowToStep dicts, strings, or nested
        instructions: List[str] = []
        ins = recipe.get("recipeInstructions") or []
        if isinstance(ins, str):
            ins = [ins]

        if isinstance(ins, list):
            for step in ins:
                if isinstance(step, dict):
                    text = step.get("text") or step.get("name")
                    if text:
                        instructions.append(str(text).strip())
                elif isinstance(step, str):
                    instructions.append(step.strip())

        # Author
        author_name = None
        author = recipe.get("author")
        if isinstance(author, dict):
            author_name = author.get("name")
        elif isinstance(author, list) and author and isinstance(author[0], dict):
            author_name = author[0].get("name")

        return ExtractResult(
            name=name,
            image_url=image_url,
            ingredients_raw=self._clean_lines(ingredients),
            instructions_raw=self._clean_lines(instructions),
            servings_raw=str(servings_raw).strip() if servings_raw else None,
            cuisine=str(cuisine).strip() if cuisine else None,
            description=str(description).strip() if description else None,
            author_name=str(author_name).strip() if author_name else None,
            extract_source="jsonld",
            extract_error=None,
        )

    def _coerce_image(self, image: Any) -> Optional[str]:
        """
        image may be:
          - string URL
          - list of strings
          - dict with url
        """
        if not image:
            return None
        if isinstance(image, str):
            return image.strip()
        if isinstance(image, list):
            for x in image:
                if isinstance(x, str) and x.strip():
                    return x.strip()
                if isinstance(x, dict) and x.get("url"):
                    return str(x["url"]).strip()
            return None
        if isinstance(image, dict) and image.get("url"):
            return str(image["url"]).strip()
        return None

    # -------------------------
    # DOM fallback extraction
    # -------------------------
    def _parse_from_dom(self, response: scrapy.http.Response) -> ExtractResult:
        """
        Best-effort DOM fallback. Cookpad UI may change, so selectors are intentionally flexible.
        """
        # Name
        name = (
            response.css("h1::text").get()
            or response.css('[data-testid*="recipe-title"]::text').get()
            or self._fallback_title(response)
        )
        name = name.strip() if name else None

        # Image (best-effort)
        image_url = (
            response.css('meta[property="og:image"]::attr(content)').get()
            or response.css('img::attr(src)').get()
        )
        image_url = image_url.strip() if image_url else None

        # Ingredients:
        # Prefer structured list areas if exist; fallback to all li under “Nguyên Liệu” heading region.
        ingredients = response.css('[data-testid*="ingredients"] li::text').getall()
        if not ingredients:
            ingredients = self._extract_section_list_by_heading(
                response,
                heading_prefixes=["Nguyên Liệu", "Nguyên liệu", "Ingredients"],
            )

        # Instructions:
        instructions = response.css('[data-testid*="instructions"] li::text').getall()
        if not instructions:
            # user hint: instruction begins with "Hướng dẫn cách làm"
            instructions = self._extract_section_list_by_heading(
                response,
                heading_prefixes=["Hướng dẫn cách làm", "Cách làm", "Directions", "Instructions"],
            )

        # Servings (best-effort, usually near ingredients area)
        servings_raw = (
            response.xpath('//*[contains(normalize-space(.),"Khẩu phần")]/following::*[1]/text()').get()
            or response.xpath('//*[contains(normalize-space(.),"Servings")]/following::*[1]/text()').get()
        )
        servings_raw = servings_raw.strip() if servings_raw else None

        return ExtractResult(
            name=name,
            image_url=image_url,
            ingredients_raw=self._clean_lines(ingredients),
            instructions_raw=self._clean_lines(instructions),
            servings_raw=servings_raw,
            cuisine=None,
            description=response.css('meta[name="description"]::attr(content)').get(),
            author_name=None,
            extract_source="dom",
            extract_error=None,
        )

    def _extract_section_list_by_heading(
        self,
        response: scrapy.http.Response,
        heading_prefixes: List[str],
        max_items: int = 50,
    ) -> List[str]:
        """
        Heuristic:
          - find an element whose normalized text starts with one of heading_prefixes
          - take following list items near it

        This is intentionally loose because Cookpad markup can vary.
        """
        # Find heading node by text (any tag)
        # NOTE: normalize-space(.) is expensive but acceptable for fallback.
        for prefix in heading_prefixes:
            # Try: find node containing prefix, then fetch next ul/ol li texts.
            xp = (
                f'//*[starts-with(normalize-space(.), "{prefix}")]'
                f'/following::*[self::ul or self::ol][1]//li//text()'
            )
            items = response.xpath(xp).getall()
            items = self._clean_lines(items)
            if items:
                return items[:max_items]

            # Alternative: sometimes heading is inside h2/h3 then list is a couple siblings away
            xp2 = (
                f'//*[self::h1 or self::h2 or self::h3 or self::h4]'
                f'[starts-with(normalize-space(.), "{prefix}")]'
                f'/following-sibling::*[self::ul or self::ol][1]//li//text()'
            )
            items = response.xpath(xp2).getall()
            items = self._clean_lines(items)
            if items:
                return items[:max_items]

        return []

    def _fallback_title(self, response: scrapy.http.Response) -> Optional[str]:
        t = response.css("title::text").get()
        if not t:
            return None
        # remove trailing site name
        t = re.sub(r"\s*\|\s*Cookpad.*$", "", t).strip()
        return t or None

    # -------------------------
    # Utilities
    # -------------------------
    def _clean_lines(self, lines: Iterable[Any]) -> List[str]:
        out: List[str] = []
        for x in lines:
            if x is None:
                continue
            s = str(x)
            s = self._normalize_whitespace(s)
            if not s:
                continue
            out.append(s)
        # de-dup consecutive duplicates (common in DOM text extraction)
        deduped: List[str] = []
        prev = None
        for s in out:
            if s != prev:
                deduped.append(s)
            prev = s
        return deduped

    def _normalize_whitespace(self, s: str) -> str:
        s = s.replace("\xa0", " ")
        s = re.sub(r"[ \t\r\f\v]+", " ", s)
        s = re.sub(r"\n{2,}", "\n", s)
        return s.strip()

    def _fingerprint(self, name: str, ingredients: List[str], instructions: List[str]) -> str:
        """
        Stable fingerprint to detect content changes.
        """
        import hashlib

        payload = "\n".join([
            name.strip().lower(),
            "\n".join(i.strip().lower() for i in ingredients),
            "\n".join(i.strip().lower() for i in instructions),
        ]).encode("utf-8", errors="ignore")

        return hashlib.sha256(payload).hexdigest()
