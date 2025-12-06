import re
from pathlib import Path


ASSET_DIR = Path(__file__).resolve().parent
FILES = [
    ASSET_DIR / "index-BxgIuhub.js",
    ASSET_DIR / "RatePage-CooWzDvg.js",
    ASSET_DIR / "SpreadPage-Bj5xjxiA.js",
    ASSET_DIR / "utils-79LRmzOq.js",
]


def main() -> None:
    urls = set()
    pattern = re.compile(r"https?://[^\s\"']+")

    for file_path in FILES:
        if not file_path.exists():
            continue
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        urls.update(pattern.findall(text))

    clean_urls = []
    for raw in urls:
        url = raw.rstrip(").,;]")
        if len(url) < 15:
            continue
        clean_urls.append(url)

    for url in sorted(set(clean_urls)):
        print(url)


if __name__ == "__main__":
    main()
