from dataclasses import dataclass

@dataclass
class Page:
    url_id: int
    title: str
    summary: str
    content: str
    length: int
    hashed: str
    is_https: bool
    is_mallorca_related: bool
    last_crawled: str
