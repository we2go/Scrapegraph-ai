import os
import json
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph

# Загружаем переменные из .env файла
load_dotenv()

# Получаем ключ
anthropic_key = os.getenv("ANTHROPIC_API_KEY")
source_url = os.getenv("SCRAPE_SOURCE_URL", "https://www.instagram.com/reel/DRJVbURkZJo/")
storage_state_path = os.getenv("INSTAGRAM_STORAGE_STATE", "./instagram_state.json")

if not anthropic_key:
    print("Ошибка: ANTHROPIC_API_KEY не найден в файле .env")
    exit(1)

# Настраиваем конфигурацию графа для работы с Claude
graph_config = {
    "llm": {
        "api_key": anthropic_key,
        "model": "anthropic/claude-haiku-4-5",
    },
    "verbose": True,
    "headless": False,
    "cut": False,
    "loader_kwargs": {
        "backend": "playwright_scroll",
        "load_state": "networkidle",
        "timeout": 90,
    },
}

if os.path.exists(storage_state_path):
    graph_config["storage_state"] = storage_state_path
    print(f"Используется storage_state: {storage_state_path}")
else:
    print("storage_state не найден, запуск без авторизации Instagram")

# Создаем инстанс скрейпера
smart_scraper_graph = SmartScraperGraph(
    prompt=(
        "Return strict JSON with as many fields as available: shortcode, account_username, account_display_name, "
        "caption, hashtags[], mentions[], posted_at, likes_count, comments_count, views_count, audio_title, "
        "is_pinned_post, and comments[]. "
        "For comments include: username, text, time_ago, like_count, is_pinned, replies[]. "
        "For each reply include: username, text, time_ago, like_count. "
        "If any field is unavailable in rendered content, return null for that field. "
        "Do not invent missing data."
    ),
    source=source_url,
    config=graph_config
)

print("Запуск скрейпинга...")
try:
    # Запускаем графовый процесс
    result = smart_scraper_graph.run()
    print("\n🕸️ Результат скрейпинга:\n")
    print(json.dumps(result, indent=4, ensure_ascii=False))
except Exception as e:
    print(f"\n❌ Произошла ошибка во время выполнения: {e}")
