import hashlib
import re
from unidecode import unidecode


def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = unidecode(s)  # bo dau
    s = re.sub(r"\s+", " ", s)
    return s


def slugify_vi(s: str) -> str:
    s = normalize(s)
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = re.sub(r"-+", "-", s)
    return s[:120] if s else "recipe"


def fingerprint(name: str, ingredients: list[str], instructions: list[str]) -> str:
    base = "\n".join(
        [
            normalize(name),
            "\n".join(normalize(x) for x in (ingredients or [])),
            "\n".join(normalize(x) for x in (instructions or [])),
        ]
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def clean_lines(lines: list[str]) -> list[str]:
    out = []
    for x in lines or []:
        x = " ".join((x or "").split()).strip()
        if x:
            out.append(x)
    # dedup giu thu tu
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq
