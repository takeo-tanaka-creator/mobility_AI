"""
============================================================
generate_insights.py — 日次AIインサイト生成バッチ
============================================================
配置場所: scripts/generate_insights.py

実行タイミング:
  GitHub Actions daily_pipeline.yml の最終ステップとして実行
  （在庫データ・気象データの取得完了後）

処理フロー:
  ┌────────────────┐
  │  Supabase DB   │
  │  ・inventory    │
  │  ・weather      │──→ データ取得（直近7日）
  │  ・sales        │
  │  ・promotions   │
  └────────┬───────┘
           ▼
  ┌────────────────┐
  │  prompts.yaml  │──→ テンプレート読み込み
  └────────┬───────┘
           ▼
  ┌────────────────┐
  │  llm_client.py │──→ データ埋め込み＋LLM API呼び出し
  └────────┬───────┘
           ▼
  ┌────────────────┐
  │  Supabase DB   │
  │  ai_insights   │──→ 生成結果を保存
  └────────────────┘
"""

import os
import sys
import json
import logging
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# --- パス設定（GitHub Actions実行時のルートに合わせる）---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from app.components.llm_client import generate_daily_insight


# ============================================================
#  Supabase からデータ取得
# ============================================================
def get_supabase_client():
    """Supabaseクライアント初期化"""
    from supabase import create_client
    url = os.environ['SUPABASE_URL']
    key = os.environ['SUPABASE_KEY']
    return create_client(url, key)


def fetch_recent_data(supabase, table: str, store_id: str, days: int = 7) -> list:
    """指定テーブルから直近N日間のデータを取得"""
    since = (date.today() - timedelta(days=days)).isoformat()
    response = (
        supabase.table(table)
        .select("*")
        .gte("date", since)
        .eq("store_id", store_id)
        .order("date", desc=False)
        .execute()
    )
    return response.data


def fetch_competitor_data(supabase, competitor_names: list, days: int = 7) -> list:
    """競合の在庫データを取得"""
    since = (date.today() - timedelta(days=days)).isoformat()
    response = (
        supabase.table("inventory_data")
        .select("*")
        .gte("date", since)
        .in_("source_name", competitor_names)
        .order("date", desc=False)
        .execute()
    )
    return response.data


# ============================================================
#  インサイト生成＆保存
# ============================================================
def save_insight(supabase, store_id: str, insight_text: str, data_summary: dict):
    """生成されたインサイトをDBに保存"""
    supabase.table("ai_insights").insert({
        "date": date.today().isoformat(),
        "store_id": store_id,
        "insight_text": insight_text,
        "data_summary": json.dumps(data_summary, ensure_ascii=False),
        "model_used": os.environ.get("LLM_MODEL", "gpt-4o-mini"),
    }).execute()
    logger.info(f"✅ インサイト保存完了: {store_id}")


def run_for_store(supabase, store_id: str, store_name: str, competitors: list):
    """1店舗分のインサイト生成を実行"""
    logger.info(f"=== {store_name} のインサイト生成開始 ===")

    # ① 各データ取得
    inventory = fetch_recent_data(supabase, "inventory_data", store_id)
    weather = fetch_recent_data(supabase, "weather_data", store_id)
    sales = fetch_recent_data(supabase, "sales_data", store_id)
    promotions = fetch_recent_data(supabase, "sales_data", store_id)  # 販促費も同テーブル
    competitor_data = fetch_competitor_data(supabase, competitors)

    # ② データサマリ（保存用）
    data_summary = {
        "inventory_count": len(inventory),
        "weather_count": len(weather),
        "sales_count": len(sales),
        "promotion_count": len(promotions),
        "competitor_count": len(competitor_data),
    }
    logger.info(f"データ件数: {data_summary}")

    # ③ LLMインサイト生成
    config_path = os.path.join(PROJECT_ROOT, "config", "prompts.yaml")
    llm_model = os.environ.get("LLM_MODEL", "gpt-4o-mini")

    insight = generate_daily_insight(
        store_name=store_name,
        inventory_records=inventory,
        weather_records=weather,
        sales_records=sales,
        promotion_records=promotions,
        competitor_records=competitor_data,
        llm_model=llm_model,
        config_path=config_path,
    )

    logger.info(f"インサイト生成完了 ({len(insight)}文字)")

    # ④ DB保存
    save_insight(supabase, store_id, insight, data_summary)

    return insight


# ============================================================
#  メイン実行
# ============================================================
def main():
    """
    全対象店舗のインサイトを生成する日次バッチ処理

    環境変数:
      SUPABASE_URL    - Supabase プロジェクトURL
      SUPABASE_KEY    - Supabase サービスキー
      LLM_MODEL       - 使用LLMモデル名 (default: gpt-4o-mini)
      OPENAI_API_KEY  - OpenAI APIキー（GPT使用時）
      ANTHROPIC_API_KEY - Anthropic APIキー（Claude使用時）
      GOOGLE_API_KEY  - Google APIキー（Gemini使用時）
    """
    logger.info("===== 日次AIインサイト生成 開始 =====")

    supabase = get_supabase_client()

    # --- 対象店舗定義（本番ではDBや設定ファイルから読み込む）---
    stores = [
        {
            "store_id": "store_001",
            "store_name": "自社 本店",
            "competitors": ["競合A社", "競合B社", "競合C社"],
        },
        # 複数店舗がある場合はここに追加
    ]

    for store in stores:
        try:
            insight = run_for_store(
                supabase=supabase,
                store_id=store["store_id"],
                store_name=store["store_name"],
                competitors=store["competitors"],
            )
            # 生成結果の先頭をログに出力
            preview = insight[:200].replace('\n', ' ')
            logger.info(f"プレビュー: {preview}...")

        except Exception as e:
            logger.error(f"❌ {store['store_name']} のインサイト生成失敗: {e}")
            continue

    logger.info("===== 日次AIインサイト生成 完了 =====")


if __name__ == "__main__":
    main()
