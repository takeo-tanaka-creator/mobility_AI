import os
import re
import time
import json
import yaml
import logging
from datetime import date
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict

import requests
from bs4 import BeautifulSoup
from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class InventoryResult:
    date: str
    store_id: str
    source_name: str
    source_type: str
    source_url: str
    inventory_count: Optional[int]
    title_tag: str
    status: str
    error_message: str = ""


class CarSensorCollector:
    SLEEP_SEC = 5
    TIMEOUT = 15

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    }

    DISALLOWED_PATHS = [
        "/usedcar/search.php", "/usedcar/inquiry", "/usedcar/mylist",
        "/usedcar/fair/", "/mkt/", "/cgi-bin/", "/member/",
    ]

    def __init__(self, targets: List[Dict[str, str]]):
        self.targets = targets
        self.results: List[InventoryResult] = []

    def _is_allowed(self, url: str) -> bool:
        for path in self.DISALLOWED_PATHS:
            if path in url:
                return False
        return True

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        if not self._is_allowed(url):
            logger.warning(f"robots.txt Disallow対象: {url}")
            return None

        try:
            resp = requests.get(
                url,
                headers=self.HEADERS,
                timeout=self.TIMEOUT,
                allow_redirects=True
            )
            resp.encoding = resp.apparent_encoding

            if resp.status_code != 200:
                logger.error(f"HTTP {resp.status_code}: {url}")
                return None

            return BeautifulSoup(resp.text, "html.parser")

        except requests.exceptions.RequestException as e:
            logger.error(f"リクエストエラー: {e}")
            return None

    def _extract_inventory(self, soup: BeautifulSoup) -> Optional[int]:
        result_bar = soup.find(class_="resultBar__result")
        if result_bar:
            text = result_bar.get_text(strip=True)
            match = re.search(r"(\d[\d,]*)\s*台", text)
            if match:
                return int(match.group(1).replace(",", ""))

        js_bar = soup.find(id="js-resultBar")
        if js_bar:
            text = js_bar.get_text(strip=True)
            match = re.search(r"(\d[\d,]*)\s*台", text)
            if match:
                return int(match.group(1).replace(",", ""))

        body_text = soup.get_text()
        match = re.search(r"掲載台数\s*[：:\s]*(\d[\d,]*)\s*台", body_text)
        if match:
            return int(match.group(1).replace(",", ""))

        detail_links = soup.find_all("a", href=re.compile(r"/usedcar/detail/"))
        if detail_links:
            unique = set(a.get("href", "") for a in detail_links)
            if len(unique) > 0:
                return len(unique)

        return None

    def collect_single(self, target: Dict[str, str]) -> InventoryResult:
        today = date.today().isoformat()
        store_id = target["store_id"]
        name = target["name"]
        source_type = target.get("type", "competitor")
        url = target["url"]

        logger.info(f"取得開始: {name} → {url}")

        soup = self._fetch(url)
        if soup is None:
            return InventoryResult(
                date=today,
                store_id=store_id,
                source_name=name,
                source_type=source_type,
                source_url=url,
                inventory_count=None,
                title_tag="",
                status="error",
                error_message="ページ取得失敗"
            )

        title = soup.find("title").get_text(strip=True) if soup.find("title") else ""

        if "error" in title.lower() or "エラー" in title:
            return InventoryResult(
                date=today,
                store_id=store_id,
                source_name=name,
                source_type=source_type,
                source_url=url,
                inventory_count=None,
                title_tag=title,
                status="error",
                error_message="エラーページ返却"
            )

        count = self._extract_inventory(soup)
        if count is not None:
            return InventoryResult(
                date=today,
                store_id=store_id,
                source_name=name,
                source_type=source_type,
                source_url=url,
                inventory_count=count,
                title_tag=title,
                status="success"
            )

        return InventoryResult(
            date=today,
            store_id=store_id,
            source_name=name,
            source_type=source_type,
            source_url=url,
            inventory_count=None,
            title_tag=title,
            status="no_data",
            error_message="在庫台数抽出失敗"
        )

    def collect_all(self) -> List[InventoryResult]:
        self.results = []
        for i, target in enumerate(self.targets):
            if i > 0:
                logger.info(f"⏳ {self.SLEEP_SEC}秒待機...")
                time.sleep(self.SLEEP_SEC)

            result = self.collect_single(target)
            self.results.append(result)

        return self.results

    def to_dict_list(self) -> List[dict]:
        return [asdict(r) for r in self.results]

    def to_json(self) -> str:
        return json.dumps(self.to_dict_list(), ensure_ascii=False, indent=2)

    def summary(self) -> str:
        lines = [
            "",
            "=" * 70,
            f"  📊 カーセンサー在庫台数 取得サマリ ({date.today().isoformat()})",
            "=" * 70,
            f"  {'store_id':<15s} | {'店舗名':<20s} | {'在庫台数':>8s} | ステータス",
            "-" * 70,
        ]
        for r in self.results:
            icon = "✅" if r.status == "success" else "⚠️"
            cnt = f"{r.inventory_count}台" if r.inventory_count is not None else "---"
            lines.append(
                f"  {icon} {r.store_id:<13s} | {r.source_name:<20s} | {cnt:>8s} | {r.status}"
            )
        lines.append("=" * 70)
        return "\n".join(lines)


def load_targets(config_path: str = "config/targets.yaml") -> List[Dict[str, str]]:
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    stores = config.get("stores", [])
    if not stores:
        raise ValueError("config/targets.yaml に stores が定義されていません")

    return stores


def save_to_supabase(results: List[InventoryResult]):
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    supabase = create_client(url, key)

    payload = []
    for r in results:
        payload.append({
            "date": r.date,
            "store_id": r.store_id,
            "source_name": r.source_name,
            "source_type": r.source_type,
            "source_url": r.source_url,
            "inventory_count": r.inventory_count,
            "title_tag": r.title_tag,
            "status": r.status,
            "error_message": r.error_message,
        })

    if not payload:
        logger.warning("保存対象データがありません")
        return

    supabase.table("inventory_data").upsert(
        payload,
        on_conflict="date,store_id"
    ).execute()

    logger.info(f"✅ Supabase保存完了: {len(payload)}件")


def main():
    logger.info("===== カーセンサー在庫収集 開始 =====")

    targets = load_targets("config/targets.yaml")
    collector = CarSensorCollector(targets)

    results = collector.collect_all()

    print(collector.summary())
    print("\n📄 JSON出力:")
    print(collector.to_json())

    save_to_supabase(results)

    logger.info("===== カーセンサー在庫収集 完了 =====")


if __name__ == "__main__":
    main()
