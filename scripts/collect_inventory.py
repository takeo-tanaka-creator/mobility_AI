"""
==========================================================================
カーセンサー 在庫台数収集スクリプト v2（本番用）
==========================================================================

【根拠】
- robots.txt: shop-stocklist.xml が Sitemap として公開されており、
  stocklistページは Disallow に含まれていない → 検索エンジン向け公開ページ
- 取得方式: 1ページ目のみアクセスし、resultBar__result 要素の
  「XX台」テキストから在庫総台数を取得（ページネーション不要）
- サーバー配慮: リクエスト間隔5秒以上、User-Agent明示

【取得ポイント】
  stocklistページ内の <div class="resultBar__result"> 要素
  → 「54台」のように在庫総数が1要素で表示される
  → 1ページ目のHTMLだけで在庫総数が取得可能
==========================================================================
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import json
import logging
from datetime import date
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class InventoryResult:
    """在庫取得結果"""
    date: str
    source_name: str
    source_url: str
    inventory_count: Optional[int]
    title_tag: str
    status: str  # success / error / no_data
    error_message: str = ""


class CarSensorCollector:
    """カーセンサー在庫台数コレクター v2"""

    SLEEP_SEC = 5
    TIMEOUT = 15

    HEADERS = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
    }

    # robots.txt Disallow リスト（簡易チェック用）
    DISALLOWED_PATHS = [
        '/usedcar/search.php', '/usedcar/inquiry', '/usedcar/mylist',
        '/usedcar/fair/', '/mkt/', '/cgi-bin/', '/member/',
    ]

    def __init__(self, targets: List[Dict[str, str]]):
        """
        Args:
            targets: [
                {"name": "自社A店", "url": "https://www.carsensor.net/shop/.../stocklist/"},
                {"name": "競合B店", "url": "https://www.carsensor.net/shop/.../stocklist/"},
            ]
        """
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
            resp = requests.get(url, headers=self.HEADERS,
                                timeout=self.TIMEOUT, allow_redirects=True)
            resp.encoding = resp.apparent_encoding
            if resp.status_code != 200:
                logger.error(f"HTTP {resp.status_code}: {url}")
                return None
            return BeautifulSoup(resp.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"リクエストエラー: {e}")
            return None

    def _extract_inventory(self, soup: BeautifulSoup) -> Optional[int]:
        """
        在庫台数の抽出（優先順位付き）

        P1: <div class="resultBar__result"> → 「54台」
        P2: <div id="js-resultBar"> → 「54台スライドショーで...」
        P3: ページ内テキストから「掲載台数XX台」パターン
        P4: フォールバック — usedcar/detail リンクのカウント
        """

        # ---- P1: resultBar__result (最も信頼性が高い) ----
        result_bar = soup.find(class_='resultBar__result')
        if result_bar:
            text = result_bar.get_text(strip=True)
            match = re.search(r'(\d[\d,]*)\s*台', text)
            if match:
                count = int(match.group(1).replace(',', ''))
                logger.info(f"[P1] resultBar__result → {count}台")
                return count

        # ---- P2: js-resultBar ----
        js_bar = soup.find(id='js-resultBar')
        if js_bar:
            text = js_bar.get_text(strip=True)
            match = re.search(r'(\d[\d,]*)\s*台', text)
            if match:
                count = int(match.group(1).replace(',', ''))
                logger.info(f"[P2] js-resultBar → {count}台")
                return count

        # ---- P3: 「掲載台数XX台」テキスト ----
        body_text = soup.get_text()
        match = re.search(r'掲載台数\s*[：:\s]*(\d[\d,]*)\s*台', body_text)
        if match:
            count = int(match.group(1).replace(',', ''))
            logger.info(f"[P3] 掲載台数テキスト → {count}台")
            return count

        # ---- P4: 車両詳細リンク数 (フォールバック) ----
        detail_links = soup.find_all('a', href=re.compile(r'/usedcar/detail/'))
        if detail_links:
            unique = set(a.get('href', '') for a in detail_links)
            count = len(unique)
            if count > 0:
                logger.info(f"[P4] 詳細リンクカウント → {count}台（※1ページ分のみ）")
                return count

        return None

    def collect_single(self, name: str, url: str) -> InventoryResult:
        today = date.today().isoformat()
        logger.info(f"取得開始: {name} → {url}")

        soup = self._fetch(url)
        if soup is None:
            return InventoryResult(
                date=today, source_name=name, source_url=url,
                inventory_count=None, title_tag="",
                status="error", error_message="ページ取得失敗"
            )

        title = (soup.find('title').get_text(strip=True)
                 if soup.find('title') else "")

        if 'error' in title.lower() or 'エラー' in title:
            return InventoryResult(
                date=today, source_name=name, source_url=url,
                inventory_count=None, title_tag=title,
                status="error", error_message="エラーページ返却"
            )

        count = self._extract_inventory(soup)
        if count is not None:
            return InventoryResult(
                date=today, source_name=name, source_url=url,
                inventory_count=count, title_tag=title,
                status="success"
            )

        return InventoryResult(
            date=today, source_name=name, source_url=url,
            inventory_count=None, title_tag=title,
            status="no_data", error_message="在庫台数抽出失敗"
        )

    def collect_all(self) -> List[InventoryResult]:
        self.results = []
        for i, t in enumerate(self.targets):
            if i > 0:
                logger.info(f"⏳ {self.SLEEP_SEC}秒待機...")
                time.sleep(self.SLEEP_SEC)
            result = self.collect_single(t['name'], t['url'])
            self.results.append(result)
        return self.results

    def to_dict_list(self) -> List[dict]:
        return [asdict(r) for r in self.results]

    def to_json(self) -> str:
        return json.dumps(self.to_dict_list(), ensure_ascii=False, indent=2)

    def summary(self) -> str:
        lines = [
            "",
            "=" * 65,
            f"  📊 カーセンサー在庫台数 取得サマリ ({date.today().isoformat()})",
            "=" * 65,
            f"  {'店舗名':<22s} | {'在庫台数':>8s} | ステータス",
            "-" * 65,
        ]
        for r in self.results:
            icon = "✅" if r.status == "success" else "⚠️"
            cnt = f"{r.inventory_count}台" if r.inventory_count is not None else "---"
            lines.append(f"  {icon} {r.source_name:<20s} | {cnt:>8s} | {r.status}")
        lines.append("=" * 65)
        return "\n".join(lines)


# ============================================================
#  Supabase 書き込み関数（スケルトン）
# ============================================================
def save_to_supabase(results: List[InventoryResult]):
    """
    Supabaseへの書き込み処理（本番実装時に有効化）

    from supabase import create_client
    url = os.environ['SUPABASE_URL']
    key = os.environ['SUPABASE_KEY']
    supabase = create_client(url, key)

    for r in results:
        supabase.table('inventory_data').insert({
            'date': r.date,
            'source_name': r.source_name,
            'source_url': r.source_url,
            'inventory_count': r.inventory_count,
            'title_tag': r.title_tag,
            'status': r.status,
            'error_message': r.error_message,
        }).execute()
    """
    logger.info(f"[STUB] Supabase保存: {len(results)}件")


# ============================================================
#  メイン実行
# ============================================================
if __name__ == "__main__":
    # --- 取得対象の定義（実運用ではDBまたは設定ファイルから読み込む） ---
    targets = [
        {
            "name": "ケーユー函館店",
            "url": "https://www.carsensor.net/shop/hokkaido/050110104/stocklist/"
        },
        {
            "name": "ケーユー旭川店",
            "url": "https://www.carsensor.net/shop/hokkaido/050110107/stocklist/"
        },
        {
            "name": "ケーユー帯広店",
            "url": "https://www.carsensor.net/shop/hokkaido/050110112/stocklist/"
        },
    ]

    collector = CarSensorCollector(targets)
    results = collector.collect_all()

    # サマリ表示
    print(collector.summary())

    # JSON出力
    print("\n📄 JSON出力:")
    print(collector.to_json())

    # Supabase保存（スケルトン）
    save_to_supabase(results)
