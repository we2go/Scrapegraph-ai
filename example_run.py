import os
import json
from datetime import datetime
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

def save_report(result: dict, url: str) -> str:
    """Сохраняет результат в history/ как Markdown-отчёт (Obsidian) + JSON."""
    history_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history")
    os.makedirs(history_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    ts_display = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    shortcode = result.get("shortcode") or url.rstrip("/").split("/")[-1]
    base_name = f"{ts}_{shortcode}"

    json_path = os.path.join(history_dir, f"{base_name}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    md_path = os.path.join(history_dir, f"{base_name}.md")
    md = []

    def v(key):
        val = result.get(key)
        return val if val is not None else "—"

    username = v("account_username")
    display_name = v("account_display_name")

    md.append("---")
    md.append(f"shortcode: {shortcode}")
    md.append(f'account: "@{username}"')
    md.append(f'posted_at: "{v("posted_at")}"')
    md.append(f'scraped_at: "{ts_display}"')
    md.append(f"likes: {v('likes_count')}")
    md.append(f"comments: {v('comments_count')}")
    md.append(f"views: {v('views_count')}")
    md.append(f'source: "{url}"')
    md.append("tags: [instagram, scrape]")
    md.append("---")
    md.append("")
    md.append(f"# Instagram Reel — @{username}")
    md.append("")
    md.append(f"> Scrape: {ts_display} · [Open in Instagram]({url})")
    md.append("")
    md.append("## Метаданные")
    md.append("")
    md.append("| Поле | Значение |")
    md.append("|------|----------|")
    md.append(f"| **Shortcode** | `{shortcode}` |")
    md.append(f"| **Аккаунт** | @{username} ({display_name}) |")
    md.append(f"| **Дата публикации** | {v('posted_at')} |")
    md.append(f"| **Аудио** | {v('audio_title')} |")
    md.append(f"| **Закреплён** | {v('is_pinned_post')} |")
    md.append(f"| ❤️ **Лайки** | **{v('likes_count')}** |")
    md.append(f"| 💬 **Комментарии** | **{v('comments_count')}** |")
    md.append(f"| 👁 **Просмотры** | {v('views_count')} |")
    md.append("")

    caption = result.get("caption") or "—"
    md.append("## Подпись")
    md.append("")
    md.append(f"> {caption}")
    md.append("")

    hashtags = result.get("hashtags") or []
    mentions = result.get("mentions") or []
    if hashtags or mentions:
        md.append("## Теги")
        md.append("")
        if hashtags:
            md.append(f"**Хэштеги:** {' '.join(f'`{h}`' for h in hashtags)}")
        if mentions:
            md.append(f"**Упоминания:** {' '.join(f'`{m}`' for m in mentions)}")
        md.append("")

    comments = result.get("comments") or []
    total_replies = sum(len(c.get("replies") or []) for c in comments)
    md.append(f"## Комментарии ({len(comments)})")
    if total_replies:
        md.append(f"*Включая {total_replies} ответов (replies)*")
    md.append("")

    for i, c in enumerate(comments, 1):
        pinned = " 📌" if c.get("is_pinned") else ""
        like_count = c.get("like_count")
        likes_str = f" · ❤️ {like_count}" if like_count else ""
        time_ago = c.get("time_ago", "")
        time_str = f"{time_ago}" if time_ago else ""
        md.append(f"### {i}. @{c.get('username', '?')}{pinned}")
        md.append(f"*{time_str}{likes_str}*")
        md.append("")
        md.append(f"{c.get('text', '')}")
        md.append("")

        replies = c.get("replies") or []
        if replies:
            for j, r in enumerate(replies, 1):
                r_likes = r.get("like_count")
                r_likes_str = f" · ❤️ {r_likes}" if r_likes else ""
                r_time = r.get("time_ago", "")
                md.append(f"  > **↳ @{r.get('username', '?')}** {r_time}{r_likes_str}")
                md.append(f"  > {r.get('text', '')}")
                md.append("")

    md.append("---")
    md.append(f"*Сгенерировано автоматически · {ts_display}*")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    return md_path


print("Запуск скрейпинга...")
try:
    # Запускаем графовый процесс
    raw = smart_scraper_graph.run()
    # ScrapeGraphAI может обернуть результат в {"content": {...}}
    if isinstance(raw, dict) and "content" in raw and len(raw) == 1:
        result = raw["content"]
    else:
        result = raw
    print("\n🕸️ Результат скрейпинга:\n")
    print(json.dumps(result, indent=4, ensure_ascii=False))

    report_path = save_report(result, source_url)
    print(f"\n📄 Отчёт сохранён: {report_path}")
except Exception as e:
    print(f"\n❌ Произошла ошибка во время выполнения: {e}")
