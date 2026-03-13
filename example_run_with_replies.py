"""
Скрипт для скрейпинга Instagram Reel с раскрытием ответов на комментарии.

Архитектура: TOOLS-FIRST → LLM FALLBACK
1. [TOOLS] Playwright открывает страницу, скроллит, кликает «View replies» / «Load more».
2. [TOOLS] Regex state-machine парсит чистый текст: username → text → time → likes → replies.
3. [LLM FALLBACK] Если tools-парсер нашёл < 5 комментариев — отправляет компактный
   JSON error-context в LLM-advisor, затем fallback на чанковую LLM-экстракцию.

LLM-Advisor: вызывается ТОЛЬКО при ошибке tools-шага. Получает короткий JSON-контекст
проблемы, анализирует, предлагает решение (click_text, scroll, JS eval, etc.).
"""

import asyncio
import json
import os
import re
import traceback

import anthropic
from dotenv import load_dotenv

load_dotenv()

anthropic_key = os.getenv("ANTHROPIC_API_KEY")
source_url = os.getenv(
    "SCRAPE_SOURCE_URL", "https://www.instagram.com/p/DRJVbURkZJo/"
)
storage_state_path = os.getenv("INSTAGRAM_STORAGE_STATE", "./instagram_state.json")

# Instagram /reel/ URL перенаправляет на ленту Reels, /p/ открывает пост с комментариями
if "/reel/" in source_url:
    source_url = source_url.replace("/reel/", "/p/")
    print(f"⚠️  /reel/ → /p/ (чтобы видеть комментарии): {source_url}")

# ── Конфигурация скрейпинга (через .env или значения по умолчанию) ──
MAX_EXPAND_ROUNDS = int(os.getenv("MAX_EXPAND_ROUNDS", "30"))     # макс. раундов раскрытия комментариев
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "2.0"))            # пауза между скроллами (сек)
ZERO_ROUNDS_LIMIT = int(os.getenv("ZERO_ROUNDS_LIMIT", "3"))      # стоп после N раундов без кликов
HEADLESS = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
SAVE_DEBUG_HTML = os.getenv("SAVE_DEBUG_HTML", "false").lower() in ("1", "true", "yes")

if not anthropic_key:
    print("Ошибка: ANTHROPIC_API_KEY не найден в файле .env")
    exit(1)

# ── Anthropic client для LLM-advisor ──
_anthropic_client = anthropic.Anthropic(api_key=anthropic_key)


def _resolve_advisor_model() -> str:
    """
    Автоматически находит лучшую доступную модель через Anthropic API.
    Приоритет: haiku (дешёвая/быстрая) → sonnet → opus.
    Если API недоступен — фолбэк на ручной перебор.
    """
    override = os.getenv("LLM_ADVISOR_MODEL")
    if override:
        print(f"  LLM-advisor модель (из .env): {override}")
        return override

    # 1. Пытаемся получить список моделей через API
    try:
        models = _anthropic_client.models.list(limit=50)
        available = [m.id for m in models.data]
        print(f"  Доступные модели ({len(available)}): {', '.join(available)}")

        # Ищем по приоритету: haiku → sonnet (дешевле для advisor)
        for prefix in ["claude-haiku", "claude-sonnet-4-", "claude-3-haiku"]:
            for mid in available:
                if mid.startswith(prefix):
                    print(f"  LLM-advisor модель (auto): {mid}")
                    return mid

        # Любая доступная
        if available:
            print(f"  LLM-advisor модель (fallback): {available[0]}")
            return available[0]
    except Exception as e:
        print(f"  ⚠️ models.list() недоступен: {e}")

    # 2. Фолбэк — пробуем модели по одной
    candidates = [
        "claude-haiku-4-5-20251001",
        "claude-sonnet-4-6"
    ]
    for cand in candidates:
        try:
            _anthropic_client.messages.create(
                model=cand, max_tokens=5,
                messages=[{"role": "user", "content": "test"}],
            )
            print(f"  LLM-advisor модель (probe): {cand}")
            return cand
        except Exception:
            continue

    raise RuntimeError("Не удалось найти ни одну рабочую модель Anthropic")


LLM_ADVISOR_MODEL = _resolve_advisor_model()


# ── LLM-Advisor: диагностика + адаптивное решение проблем ───────────────

async def gather_page_diagnostics(page, max_items: int = 30) -> dict:
    """
    Собирает структурированную диагностику текущего состояния страницы:
    кнопки, ссылки, интерактивные элементы, сэмпл текста.
    """
    diag: dict = {
        "url": page.url,
        "title": await page.title(),
        "buttons": [],
        "links": [],
        "interactive_elements": [],
        "page_text_start": "",
        "page_text_end": "",
    }

    # Кнопки
    try:
        btns = page.locator("button, [role='button']")
        for i in range(min(await btns.count(), max_items)):
            try:
                txt = (await btns.nth(i).inner_text(timeout=800)).strip()
                if txt and len(txt) < 200:
                    diag["buttons"].append(txt)
            except Exception:
                pass
    except Exception:
        pass

    # Ссылки
    try:
        links = page.locator("a")
        for i in range(min(await links.count(), max_items)):
            try:
                txt = (await links.nth(i).inner_text(timeout=800)).strip()
                if txt and len(txt) < 200:
                    diag["links"].append(txt)
            except Exception:
                pass
    except Exception:
        pass

    # Интерактивные элементы с aria-label / role
    try:
        snippet = await page.evaluate("""() => {
            const sels = '[role="button"], [role="link"], [aria-label], span[tabindex]';
            const els = document.querySelectorAll(sels);
            return Array.from(els).slice(0, 40).map(el => ({
                tag: el.tagName,
                text: (el.innerText || '').substring(0, 120),
                role: el.getAttribute('role'),
                ariaLabel: el.getAttribute('aria-label'),
            }));
        }""")
        diag["interactive_elements"] = snippet
    except Exception:
        pass

    # Текст страницы (начало + конец)
    try:
        body_text = await page.inner_text("body", timeout=5000)
        diag["page_text_start"] = body_text[:3000]
        diag["page_text_end"] = body_text[-2000:] if len(body_text) > 3000 else ""
    except Exception:
        pass

    return diag


def ask_llm_advisor(problem_context: dict) -> dict:
    """
    Отправляет диагностику проблемы в LLM и получает структурированный JSON с действиями.

    problem_context должен содержать:
      - phase: "open_comments" | "expand_replies" | "quality_check"
      - problem: описание проблемы
      - diagnostics: результат gather_page_diagnostics()
      - attempts: что уже пробовали

    Возвращает:
      {
        "actions": [{"type": "click_text"|"click_selector"|"scroll"|"wait"|"evaluate_js"|"skip",
                      "value": "...", "description": "..."}],
        "explanation": "..."
      }
    """
    system_prompt = """\
You are an expert Instagram web scraping advisor. You receive diagnostic info about a mobile page state and a problem description.
Return ONLY valid JSON (no markdown fences, no text outside JSON):
{
  "actions": [
    {"type": "<action_type>", "value": "<value>", "description": "<what this does>"}
  ],
  "explanation": "<brief reasoning>"
}

Action types:
- "click_text": click element containing this exact text (case-insensitive). Best for visible text.
- "click_selector": click element matching this CSS selector.
- "scroll": scroll page by N pixels down (positive int).
- "wait": wait N seconds (max 10).
- "evaluate_js": run safe JS snippet to interact with page (no fetch/XHR/import). Max 400 chars.
- "skip": no action needed; explain why.

CRITICAL RULES:
- PREFER click_text over scroll. If page_text contains clickable text like "Смотреть все ответы (N)", return MULTIPLE click_text actions for EACH unique instance.
- Return up to 10 actions per response. Click buttons FIRST, scroll AFTER.
- For "expand_replies" phase: look for ALL "Смотреть все ответы (N)" patterns in page_text and create a click_text for each distinct one.
- For "open_comments" phase: look for "Посмотреть все комментарии" or "View all N comments".
- Do NOT just scroll if there are visible clickable elements in the page text.

Instagram mobile specifics:
- Comments section is behind "Посмотреть все комментарии (N)" or "View all N comments" — usually a <span> or <a>.
- "View replies (N)" / "Смотреть все ответы (N)" / "Посмотреть ответы (N)" expands reply threads.
- "+" circle button or "Load more comments" loads next batch.
- Elements often have obfuscated class names; prefer text selectors and aria-labels.
- Page text may be in Russian."""

    user_msg = json.dumps(problem_context, ensure_ascii=False, indent=2)
    # Обрезаем если слишком длинный (лимит контекста)
    if len(user_msg) > 12000:
        user_msg = user_msg[:12000] + "\n... (truncated)"

    try:
        resp = _anthropic_client.messages.create(
            model=LLM_ADVISOR_MODEL,
            max_tokens=1500,
            temperature=0.1,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        content = resp.content[0].text.strip()
        # Убираем возможные markdown-обёртки
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?\s*", "", content)
            content = re.sub(r"\s*```\s*$", "", content)
        result = json.loads(content)
        print(f"  🤖 LLM-advisor: {result.get('explanation', '')}")
        return result
    except json.JSONDecodeError:
        print(f"  ⚠️ LLM-advisor вернул невалидный JSON: {content[:200]}")
        return {"actions": [], "explanation": "Invalid JSON from LLM"}
    except Exception as e:
        print(f"  ⚠️ LLM-advisor ошибка: {e}")
        return {"actions": [], "explanation": f"Error: {e}"}


async def execute_llm_actions(page, actions: list) -> int:
    """
    Безопасно выполняет действия, предложенные LLM-advisor.
    Возвращает количество успешных действий.
    """
    executed = 0
    for action in actions:
        atype = action.get("type", "")
        value = action.get("value", "")
        desc = action.get("description", "")

        try:
            if atype == "click_text":
                # Несколько стратегий поиска элемента по тексту
                found = False
                strategies = [
                    page.locator(f"text='{value}'").first,
                    page.locator(f"text=/{re.escape(value)}/i").first,
                    page.get_by_text(value, exact=False),
                ]
                for el in strategies:
                    try:
                        if await el.count() > 0:
                            await el.click(timeout=5000)
                            print(f"    🤖 → click_text '{value}': OK — {desc}")
                            executed += 1
                            found = True
                            await asyncio.sleep(2)
                            break
                    except Exception:
                        pass
                if not found:
                    print(f"    🤖 → click_text '{value}': не найден")

            elif atype == "click_selector":
                el = page.locator(value).first
                if await el.count() > 0:
                    await el.click(timeout=5000)
                    print(f"    🤖 → click_selector: OK — {desc}")
                    executed += 1
                    await asyncio.sleep(2)
                else:
                    print(f"    🤖 → click_selector '{value}': не найден")

            elif atype == "scroll":
                amount = int(value) if str(value).lstrip("-").isdigit() else 1000
                await page.mouse.wheel(0, amount)
                print(f"    🤖 → scroll {amount}px — {desc}")
                executed += 1
                await asyncio.sleep(1)

            elif atype == "wait":
                secs = min(float(value) if value else 2, 10)
                await asyncio.sleep(secs)
                print(f"    🤖 → wait {secs}s — {desc}")
                executed += 1

            elif atype == "evaluate_js":
                # Безопасность: ограничиваем длину и запрещаем сетевые вызовы
                forbidden = ["fetch", "xmlhttp", "import(", "require(", "eval("]
                if len(value) < 500 and not any(kw in value.lower() for kw in forbidden):
                    await page.evaluate(value)
                    print(f"    🤖 → JS eval: OK — {desc}")
                    executed += 1
                    await asyncio.sleep(1)
                else:
                    print(f"    🤖 → JS eval: отклонён (безопасность)")

            elif atype == "skip":
                print(f"    🤖 → skip — {desc}")

        except Exception as e:
            print(f"    🤖 → {atype} ошибка: {e}")

    return executed


def check_extraction_quality(result: dict, expected_comments_hint: int | None = None) -> dict:
    """
    Проверяет качество извлечённых данных. Возвращает диагностику:
      {"ok": bool, "issues": [...], "suggestion": "..."}
    Если quality плохой — вызывает LLM-advisor для анализа.
    """
    issues = []
    comments = result.get("comments") or []
    n = len(comments)

    # 1. Мало комментариев
    if expected_comments_hint and n < expected_comments_hint * 0.3:
        issues.append(f"Извлечено {n} комментариев, ожидалось ~{expected_comments_hint}")
    elif n == 0:
        issues.append("Комментарии не извлечены")

    # 2. Все like_count == null
    likes = [c.get("like_count") for c in comments]
    if comments and all(lk is None for lk in likes):
        issues.append("Ни у одного комментария нет like_count")

    # 3. Нет replies ни у кого
    total_replies = sum(len(c.get("replies") or []) for c in comments)
    if n > 10 and total_replies == 0:
        issues.append(f"{n} комментариев, но 0 replies — возможно replies не раскрыты")

    # 4. Ключевые поля поста = null
    for field in ["likes_count", "comments_count", "shortcode"]:
        if result.get(field) is None:
            issues.append(f"Поле {field} = null")

    if not issues:
        return {"ok": True, "issues": [], "suggestion": ""}

    # Спрашиваем LLM-advisor
    problem_ctx = {
        "phase": "quality_check",
        "problem": "Результат извлечения данных имеет проблемы",
        "issues": issues,
        "extracted_comments_count": n,
        "expected_comments_hint": expected_comments_hint,
        "sample_comments": comments[:5] if comments else [],
        "post_metadata": {k: result.get(k) for k in
                         ["shortcode", "likes_count", "comments_count", "views_count",
                          "account_username", "caption"]},
    }
    advice = ask_llm_advisor(problem_ctx)

    return {
        "ok": False,
        "issues": issues,
        "suggestion": advice.get("explanation", ""),
        "actions": advice.get("actions", []),
    }

async def expand_comments_and_replies(
    url: str,
    storage_state: str | None = None,
    headless: bool = False,
    max_expand_rounds: int = 30,
    scroll_pause: float = 2.0,
    zero_rounds_limit: int = 3,
) -> str:
    """
    Открывает Instagram-страницу в мобильном браузере (iPhone эмуляция),
    скроллит зону комментариев, кликает кнопки раскрытия ответов и подгрузки.
    Возвращает итоговый HTML страницы.
    """
    from playwright.async_api import async_playwright
    from undetected_playwright import Malenia

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)

        # Мобильный контекст — iPhone 14 Pro Max
        iphone = p.devices["iPhone 14 Pro Max"]
        context_kwargs = {**iphone}
        if storage_state and os.path.exists(storage_state):
            context_kwargs["storage_state"] = storage_state
            print(f"Используется storage_state: {storage_state}")
        else:
            print("storage_state не найден, запуск без авторизации")

        context = await browser.new_context(**context_kwargs)
        await Malenia.apply_stealth(context)
        page = await context.new_page()

        print(f"Открываю (mobile) {url} ...")
        await page.goto(url, wait_until="domcontentloaded")
        await asyncio.sleep(5)

        print(f"  Заголовок: {await page.title()}")
        print(f"  URL: {page.url}")

        # ── Закрываем модалки ──
        cookie_closed = False
        # Кнопки через role
        for txt in [
            "Разрешить все cookie",
            "Allow all cookies",
            "Отклонить необязательные файлы cookie",
            "Decline optional cookies",
            "Accept all cookies",
            "Принять все",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(re.escape(txt), re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    print(f"  Куки закрыты: {txt}")
                    cookie_closed = True
                    await asyncio.sleep(2)
                    break
            except Exception:
                pass

        # Кнопки через текст
        if not cookie_closed:
            for txt in ["cookie", "Cookie", "Разрешить", "Allow", "Принять"]:
                try:
                    btns = page.locator(f"button:has-text('{txt}')")
                    if await btns.count() > 0:
                        await btns.first.click(timeout=5000)
                        print(f"  Куки закрыты (text): {txt}")
                        cookie_closed = True
                        await asyncio.sleep(2)
                        break
                except Exception:
                    pass

        # «Not Now» / «Не сейчас» модалка логина
        for txt in ["Not Now", "Не сейчас"]:
            try:
                btn = page.get_by_role("button", name=re.compile(re.escape(txt), re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=3000)
                    print(f"  Модалка закрыта: {txt}")
                    await asyncio.sleep(1)
            except Exception:
                pass

        await asyncio.sleep(2)
        print(f"  URL после модалок: {page.url}")

        # ── Открываем секцию комментариев ──
        # В мобильной версии комментарии скрыты за «Посмотреть все комментарии (N)»
        comment_opened = False
        open_comments_patterns = [
            re.compile(r"Посмотреть\s+все\s+комментарии", re.I),
            re.compile(r"View\s+all\s+\d+\s+comments?", re.I),
            re.compile(r"View\s+all\s+comments?", re.I),
            re.compile(r"Все\s+комментарии", re.I),
        ]
        for pat in open_comments_patterns:
            # Пробуем span/a — в мобильной версии это обычно span, не button
            for tag in ["span", "a", "div"]:
                try:
                    els = page.locator(f"{tag}:text-matches('{pat.pattern}', 'i')")
                    cnt = await els.count()
                    if cnt > 0:
                        await els.first.click(timeout=5000)
                        print(f"  Открыты комментарии: {pat.pattern} ({tag})")
                        comment_opened = True
                        await asyncio.sleep(3)
                        break
                except Exception:
                    pass
            if comment_opened:
                break
            # Также через role="button"
            try:
                btn = page.get_by_role("button", name=pat)
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    print(f"  Открыты комментарии: {pat.pattern} (button)")
                    comment_opened = True
                    await asyncio.sleep(3)
                    break
            except Exception:
                pass
            # Через role="link"
            try:
                link = page.get_by_role("link", name=pat)
                if await link.count() > 0:
                    await link.first.click(timeout=5000)
                    print(f"  Открыты комментарии: {pat.pattern} (link)")
                    comment_opened = True
                    await asyncio.sleep(3)
                    break
            except Exception:
                pass

        if not comment_opened:
            # Попробуем найти по тексту «комментарии» / «comments» ближе к числу
            try:
                el = page.locator("text=/\\d+\\s*комментар/i").first
                if await el.count() > 0:
                    await el.click(timeout=5000)
                    print("  Открыты комментарии: по паттерну 'N комментар'")
                    comment_opened = True
                    await asyncio.sleep(3)
            except Exception:
                pass

        if not comment_opened:
            try:
                el = page.locator("text=/\\d+\\s*comments?/i").first
                if await el.count() > 0:
                    await el.click(timeout=5000)
                    print("  Открыты комментарии: по паттерну 'N comments'")
                    comment_opened = True
                    await asyncio.sleep(3)
            except Exception:
                pass

        if comment_opened:
            # Подождём загрузку комментариев
            await asyncio.sleep(3)
            print(f"  URL после открытия комментариев: {page.url}")
        else:
            print("  ⚠️ Кнопка «Посмотреть все комментарии» не найдена — спрашиваю LLM-advisor...")
            diag = await gather_page_diagnostics(page)
            advice = ask_llm_advisor({
                "phase": "open_comments",
                "problem": "Не удалось найти и нажать кнопку открытия комментариев. "
                           "Нужно найти элемент, который открывает секцию/панель со всеми комментариями к посту.",
                "attempts": [
                    "span/a/div с текстом 'Посмотреть все комментарии'",
                    "button role с тем же текстом",
                    "link role",
                    "regex /\\d+\\s*комментар/i",
                    "regex /\\d+\\s*comments?/i",
                ],
                "diagnostics": diag,
            })
            if advice.get("actions"):
                llm_clicks = await execute_llm_actions(page, advice["actions"])
                if llm_clicks > 0:
                    comment_opened = True
                    await asyncio.sleep(3)
                    print(f"  URL после открытия комментариев (LLM): {page.url}")

        # ── Скроллим и кликаем ──
        # В мобильной версии кнопки могут быть текстовыми ссылками/span'ами
        reply_patterns = [
            re.compile(r"View\s+replies?\s*\(\d+\)", re.I),
            re.compile(r"View\s+\d+\s+more\s+repl", re.I),
            re.compile(r"Смотреть\s+все\s+ответы", re.I),
            re.compile(r"Посмотреть\s+ответ", re.I),
            re.compile(r"Показать\s+ответ", re.I),
            re.compile(r"Ещё\s+\d+\s+ответ", re.I),
            re.compile(r"\d+\s+repl", re.I),
        ]
        load_more_patterns = [
            re.compile(r"Load\s+more\s+comments?", re.I),
            re.compile(r"View\s+more\s+comments?", re.I),
            re.compile(r"View\s+all\s+\d+\s+comments?", re.I),
            re.compile(r"Загрузить\s+ещё\s+комментар", re.I),
            re.compile(r"Ещё\s+комментар", re.I),
            re.compile(r"Посмотреть\s+все\s+\d+\s+комментар", re.I),
            re.compile(r"\+\s*$", re.I),  # кнопка «+»
        ]

        total_clicks = 0
        zero_rounds = 0

        for round_num in range(1, max_expand_rounds + 1):
            clicked_this_round = 0

            # Скроллим страницу вниз (мобильный — просто скролл)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(scroll_pause)

            # Swipe вверх для подгрузки (как на телефоне)
            await page.mouse.wheel(0, 1500)
            await asyncio.sleep(1)

            # Кликаем кнопки через role="button"
            all_patterns = load_more_patterns + reply_patterns
            for pat in all_patterns:
                try:
                    buttons = page.get_by_role("button", name=pat)
                    count = await buttons.count()
                    for i in range(count):
                        try:
                            await buttons.nth(i).click(timeout=3000)
                            clicked_this_round += 1
                            await asyncio.sleep(1.5)
                        except Exception:
                            pass
                except Exception:
                    pass

            # Кликаем span/a с текстом (мобильная верстка часто использует span)
            for pat in all_patterns:
                try:
                    spans = page.locator(f"span:text-matches('{pat.pattern}', 'i')")
                    span_count = await spans.count()
                    for i in range(span_count):
                        try:
                            await spans.nth(i).click(timeout=3000)
                            clicked_this_round += 1
                            await asyncio.sleep(1.5)
                        except Exception:
                            pass
                except Exception:
                    pass

            # Ищем ⊕ кнопку (SVG с кругом)
            try:
                plus_btns = page.locator(
                    "button svg[aria-label='Load more comments'], "
                    "button svg[aria-label='Ещё комментарии']"
                )
                for i in range(await plus_btns.count()):
                    try:
                        parent = plus_btns.nth(i).locator("xpath=ancestor::button")
                        if await parent.count() > 0:
                            await parent.first.click(timeout=3000)
                        else:
                            await plus_btns.nth(i).click(timeout=3000)
                        clicked_this_round += 1
                        await asyncio.sleep(2)
                    except Exception:
                        pass
            except Exception:
                pass

            total_clicks += clicked_this_round
            print(
                f"  Раунд {round_num}: кликов = {clicked_this_round}  "
                f"(всего = {total_clicks})"
            )

            if clicked_this_round > 0:
                zero_rounds = 0
            else:
                zero_rounds += 1
                if zero_rounds >= zero_rounds_limit:
                    # Последний шанс — спрашиваем LLM-advisor
                    print(f"  {zero_rounds} раундов без кликов → спрашиваю LLM-advisor...")
                    diag = await gather_page_diagnostics(page)
                    advice = ask_llm_advisor({
                        "phase": "expand_replies",
                        "problem": f"Уже {zero_rounds} раундов подряд ни одна кнопка не найдена. "
                                   f"Всего кликов за сессию: {total_clicks}. "
                                   "Нужно найти кнопки для подгрузки ещё комментариев или раскрытия ответов.",
                        "attempts": [
                            "role='button' с паттернами: View replies, Load more, Посмотреть ответ, Ещё комментар",
                            "span с text-matches тех же паттернов",
                            "SVG aria-label='Load more comments'",
                        ],
                        "round": round_num,
                        "total_clicks_so_far": total_clicks,
                        "diagnostics": diag,
                    })
                    if advice.get("actions"):
                        llm_clicks = await execute_llm_actions(page, advice["actions"])
                        if llm_clicks > 0:
                            total_clicks += llm_clicks
                            zero_rounds = 0  # сбрасываем — LLM нашёл что-то
                            print(f"  🤖 LLM-advisor помог: +{llm_clicks} кликов, продолжаем")
                            continue
                    print(f"  LLM-advisor не помог, завершаем раскрытие.")
                    break
                # Дополнительный скролл
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(scroll_pause + 1)

        html = await page.content()
        await browser.close()
        print(f"HTML получен: {len(html)} символов, кликов всего: {total_clicks}")

        # Сохраним HTML для диагностики (если включено)
        if SAVE_DEBUG_HTML:
            debug_html_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "history", "last_debug.html"
            )
            os.makedirs(os.path.dirname(debug_html_path), exist_ok=True)
            with open(debug_html_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  Debug HTML: {debug_html_path}")

        return html


# ── Шаг 1.5. Извлечение чистого текста из HTML ──────────────────────────

def extract_clean_text(html: str) -> str:
    """
    Извлекает чистый текст из HTML, убирает CSS/JS мусор.
    Возвращает компактный текст, пригодный для LLM.
    """
    from html.parser import HTMLParser

    class TextExtractor(HTMLParser):
        SKIP_TAGS = {"script", "style", "noscript", "meta", "link"}

        def __init__(self):
            super().__init__()
            self.texts: list[str] = []
            self._skip = False

        def handle_starttag(self, tag, attrs):
            if tag.lower() in self.SKIP_TAGS:
                self._skip = True

        def handle_endtag(self, tag):
            if tag.lower() in self.SKIP_TAGS:
                self._skip = False

        def handle_data(self, data):
            if self._skip:
                return
            t = data.strip()
            if t and len(t) > 1:
                # Отфильтровываем CSS-переменные и мусор
                if t.startswith(("--fds", "--always", "--accent", ":root", ".__fb",
                                 "@media", "rgba(", "cubic-", "linear-", ".x")):
                    return
                if len(t) > 500 and "{" in t:  # inline CSS/JS
                    return
                self.texts.append(t)

    extractor = TextExtractor()
    extractor.feed(html)
    clean = "\n".join(extractor.texts)
    print(f"  HTML → текст: {len(html)} → {len(clean)} символов ({len(clean)*100//max(len(html),1)}%)")
    return clean


# ── Шаг 2. Извлечение данных: TOOLS-FIRST (regex) → LLM fallback ────────

# Паттерны для regex-парсера (tools-подход, без LLM)
_RE_USERNAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._]{1,29}$")
_RE_TIME_AGO = re.compile(r"^(\d+)\s+(нед|дн|ч|мин|сек|мес|г)\.$")
_NAV_ITEMS = frozenset({"Главная", "Интересное", "Reels", "Сообщения", "Назад",
                       "Комментарии", "Instagram", "Поиск", "Уведомления", "Профиль",
                       "Создать", "Ещё", "Threads"})
_RE_LIKES_MULTI = re.compile(r'^Отметки\s+"Нравится":\s*(\d+)$')
_RE_LIKES_SINGLE = re.compile(r"^(\d+)\s+отметк[аи]\s+\"Нравится\"$")
_RE_AT_MENTION = re.compile(r"^@([a-zA-Z0-9._]+)")


def _build_error_context(phase: str, problem: str, **details) -> dict:
    """Компактный JSON-контекст ошибки для отправки в LLM-advisor."""
    ctx = {"phase": phase, "problem": problem}
    ctx.update(details)
    return ctx


def parse_comments_tools(text: str) -> dict:
    """
    TOOLS-FIRST: Парсит комментарии Instagram из чистого текста
    чисто regex/state-machine, БЕЗ вызова LLM.
    Формат каждого блока:
      username → [Подтвержденный] → текст → "N нед." →
      [Отметки "Нравится": N] → Ответить → Нравится →
      [Скрыть все ответы → replies...]
    """
    lines = text.split("\n")
    total_lines = len(lines)

    # ── 1. Извлекаем метаданные поста (первые ~30 строк) ──
    post_meta = {
        "shortcode": None,
        "account_username": None,
        "account_display_name": None,
        "caption": None,
        "hashtags": [],
        "mentions": [],
        "posted_at": None,
        "likes_count": None,
        "comments_count": None,
        "views_count": None,
        "audio_title": None,
        "is_pinned_post": False,
    }

    # Ищем username поста и caption в начале
    # Структура: ... → username → caption → ... → "N нед." → "Для вас" | "..."
    # Ищем маркер "Комментарии" — после него идёт автор поста
    comment_marker = -1
    header_end = min(50, total_lines)
    for i in range(header_end):
        if lines[i].strip() == "Комментарии":
            comment_marker = i
            break

    # Автор поста — первый username ПОСЛЕ "Комментарии", не из навигации
    search_start = comment_marker + 1 if comment_marker >= 0 else 0
    for i in range(search_start, header_end):
        line = lines[i].strip()
        if _RE_USERNAME.match(line) and line not in _NAV_ITEMS and not post_meta["account_username"]:
            if i + 1 < total_lines:
                next_line = lines[i + 1].strip()
                if next_line and not _RE_USERNAME.match(next_line) and next_line != "Подтвержденный":
                    post_meta["account_username"] = line
                    # Caption — всё до первого time_ago в header
                    caption_parts = []
                    for j in range(i + 1, header_end):
                        l = lines[j].strip()
                        if _RE_TIME_AGO.match(l):
                            post_meta["posted_at"] = l
                            break
                        if l in ("ещё", "..."):
                            continue
                        if l in ("Для вас", 'Значок "стрелка вниз"'):
                            break
                        caption_parts.append(l)
                    if caption_parts:
                        post_meta["caption"] = " ".join(caption_parts)
                    break
        # Ищем хэштеги
        for tag in re.findall(r"#(\w+)", line):
            if tag not in post_meta["hashtags"]:
                post_meta["hashtags"].append(f"#{tag}")
        # Ищем упоминания
        for mention in re.findall(r"@([a-zA-Z0-9._]+)", line):
            if f"@{mention}" not in post_meta["mentions"]:
                post_meta["mentions"].append(f"@{mention}")

    # Shortcode из URL (если есть в тексте)
    sc_match = re.search(r"/p/([A-Za-z0-9_-]+)", text)
    if sc_match:
        post_meta["shortcode"] = sc_match.group(1)

    # ── 2. Парсим комментарии (state machine) ──
    comments: list[dict] = []
    current: dict | None = None      # текущий комментарий
    in_replies = False                # мы в зоне replies
    parent_comment: dict | None = None  # родительский комментарий для replies
    state = "SCAN"  # SCAN → COLLECT_TEXT → AWAIT_META

    def _finalize(comment: dict):
        """Финализирует комментарий: reply (@...) → к parent, иначе → top-level."""
        nonlocal in_replies, parent_comment
        if not comment or not comment.get("username"):
            return
        if in_replies and parent_comment is not None:
            text = comment.get("text", "")
            if text.startswith("@"):
                # Это reply — добавляем к parent
                parent_comment.setdefault("replies", []).append(comment)
            else:
                # Текст без @ → выходим из reply-зоны, это новый top-level
                in_replies = False
                parent_comment = None
                comment.setdefault("replies", [])
                comments.append(comment)
        else:
            comment.setdefault("replies", [])
            comments.append(comment)

    # Пропускаем header: всё до post-author-блока (username + caption + time + "Для вас")
    start_line = 0
    for i in range(min(60, total_lines)):
        line = lines[i].strip()
        if line in ("Для вас", 'Значок "стрелка вниз"'):
            start_line = i + 1
            break
        if line == "Комментарии":
            start_line = i + 1  # минимум пропустим навигацию
    # Ещё убедимся, что пропустили post-author блок: ищем строку time_ago в header
    if start_line > 0:
        for i in range(start_line, min(start_line + 20, total_lines)):
            line = lines[i].strip()
            if line in ("Для вас", 'Значок "стрелка вниз"'):
                start_line = i + 1
                break

    i = start_line
    while i < total_lines:
        line = lines[i].strip()
        i += 1

        if not line:
            continue

        if state == "SCAN":
            # Ищем начало нового комментария: строка = username
            if line == "Скрыть все ответы":
                # Предыдущий комментарий — parent для replies
                if comments:
                    in_replies = True
                    parent_comment = comments[-1]
                continue

            if line in ("Смотреть все ответы", "Посмотреть ответы") or \
               re.match(r"Смотреть\s+все\s+ответы\s*\(\d+\)", line) or \
               re.match(r"Посмотреть\s+ответы\s*\(\d+\)", line):
                continue

            if _RE_USERNAME.match(line) and line not in _NAV_ITEMS:
                current = {
                    "username": line,
                    "text": "",
                    "time_ago": "",
                    "like_count": 0,
                    "is_pinned": False,
                    "replies": [],
                }
                state = "COLLECT_TEXT"
                continue

        elif state == "COLLECT_TEXT":
            if line == "Подтвержденный":
                # Верифицированный бейджик — пропускаем
                continue

            time_m = _RE_TIME_AGO.match(line)
            if time_m:
                current["time_ago"] = line
                state = "AWAIT_META"
                continue

            # Пропускаем мусорные строки
            if line in ("ещё", "...", 'Значок "стрелка вниз"', "Для вас"):
                continue

            # Это часть текста комментария
            if current["text"]:
                current["text"] += " " + line
            else:
                # Если текст начинается с @username — это reply
                at_m = _RE_AT_MENTION.match(line)
                if at_m and not in_replies and parent_comment is None:
                    # Standalone @mention без Скрыть все ответы — возможно, пост-caption reply
                    pass
                current["text"] = line

        elif state == "AWAIT_META":
            likes_m = _RE_LIKES_MULTI.match(line)
            likes_s = _RE_LIKES_SINGLE.match(line)

            if likes_m:
                current["like_count"] = int(likes_m.group(1))
                continue
            elif likes_s:
                current["like_count"] = int(likes_s.group(1))
                continue
            elif line == "Ответить":
                continue
            elif line == "Нравится":
                _finalize(current)
                current = None
                state = "SCAN"
                continue
            elif line == "Скрыть все ответы":
                # Финализируем текущий и он становится parent
                _finalize(current)
                current = None
                state = "SCAN"
                if comments:
                    in_replies = True
                    parent_comment = comments[-1]
                continue
            elif _RE_USERNAME.match(line) and line not in _NAV_ITEMS:
                # Следующий комментарий начался без "Нравится"
                _finalize(current)
                current = {
                    "username": line,
                    "text": "",
                    "time_ago": "",
                    "like_count": 0,
                    "is_pinned": False,
                    "replies": [],
                }
                state = "COLLECT_TEXT"
                continue

    # Финализируем последний комментарий
    if current:
        _finalize(current)

    # ── 3. Определяем reply-зоны: если текст начинается с @, это reply ──
    # (уже обработано через in_replies / parent_comment)

    # Обновляем счётчики
    total_replies = sum(len(c.get("replies", [])) for c in comments)
    post_meta["comments_count"] = len(comments)
    post_meta["comments"] = comments

    print(f"  🔧 TOOLS-парсер: {len(comments)} комментариев, {total_replies} ответов")
    return post_meta


def _llm_extract_comments(text: str) -> dict:
    """
    LLM-FALLBACK: Отправляет чистый текст в Anthropic Claude.
    Вызывается ТОЛЬКО если tools-парсер не справился.
    Использует чанкинг для больших текстов.
    """
    CHUNK_SIZE = 25_000
    MAX_TOTAL = 200_000

    if len(text) > MAX_TOTAL:
        text = text[:MAX_TOTAL]
        print(f"  ⚠️ Текст обрезан до {MAX_TOTAL} символов")

    chunks = []
    pos = 0
    while pos < len(text):
        end = min(pos + CHUNK_SIZE, len(text))
        if end < len(text):
            newline = text.rfind("\n", pos + CHUNK_SIZE - 2000, end)
            if newline > pos:
                end = newline + 1
        chunks.append(text[pos:end])
        pos = end

    print(f"  📡 LLM-fallback: текст разбит на {len(chunks)} чанков")

    base_prompt = """\
Parse Instagram post page text and return strict JSON.

RULES:
- Extract ALL comments in this text chunk.
- Each comment block follows this pattern:
  username → [Подтвержденный] → comment text → "N нед." → 'Отметки "Нравится": N' or '1 отметка "Нравится"' → "Ответить" → "Нравится"
- If "Скрыть все ответы" appears after a comment, the following comments with "@username" prefix are REPLIES to that comment.
- like_count: from 'Отметки "Нравится": 199' → 199, '1 отметка "Нравится"' → 1. No like info → 0.
- Return ONLY valid JSON (no markdown fences).
"""

    first_chunk_prompt = base_prompt + """
Return full structure:
{"shortcode": "...", "account_username": "...", "account_display_name": "...",
 "caption": "...", "hashtags": [], "mentions": [], "posted_at": "...",
 "likes_count": <int|null>, "comments_count": <int|null>, "views_count": <int|null>,
 "audio_title": null, "is_pinned_post": false,
 "comments": [{"username":"...","text":"...","time_ago":"...","like_count":<int>,"is_pinned":false,"replies":[...]}]}

TEXT:
"""

    next_chunk_prompt = base_prompt + """
Return ONLY the comments array from this chunk:
{"comments": [{"username":"...","text":"...","time_ago":"...","like_count":<int>,"is_pinned":false,"replies":[...]}]}

TEXT:
"""

    all_comments = []
    result = {}

    for idx, chunk in enumerate(chunks):
        is_first = (idx == 0)
        prompt = (first_chunk_prompt if is_first else next_chunk_prompt) + chunk
        label = f"чанк {idx+1}/{len(chunks)}"

        try:
            resp = _anthropic_client.messages.create(
                model=LLM_ADVISOR_MODEL,
                max_tokens=16000,
                temperature=0,
                messages=[{"role": "user", "content": prompt}],
            )
            content = resp.content[0].text.strip()
            if content.startswith("```"):
                content = re.sub(r"^```(?:json)?\s*", "", content)
                content = re.sub(r"\s*```\s*$", "", content)
            parsed = json.loads(content)

            chunk_comments = parsed.get("comments") or []
            all_comments.extend(chunk_comments)
            print(f"    {label}: +{len(chunk_comments)} комментариев (всего: {len(all_comments)})")

            if is_first:
                result = parsed
                result["comments"] = []

            if resp.stop_reason == "max_tokens":
                print(f"    ⚠️ {label}: output обрезан (max_tokens)")

        except json.JSONDecodeError:
            print(f"    ⚠️ {label}: невалидный JSON, пропускаю")
        except Exception as e:
            print(f"    ⚠️ {label}: ошибка: {e}")

    result["comments"] = all_comments
    if all_comments:
        result["comments_count"] = len(all_comments)

    return result


def _scrape_from_html_fallback(html_or_text: str) -> dict:
    """Фолбэк через ScrapeGraphAI если прямой вызов LLM не сработал."""
    from scrapegraphai.graphs import SmartScraperGraph

    graph_config = {
        "llm": {
            "api_key": anthropic_key,
            "model": "anthropic/claude-haiku-4-5",
        },
        "verbose": True,
        "headless": True,
        "cut": False,
    }

    scraper = SmartScraperGraph(
        prompt=(
            "Return strict JSON with: shortcode, account_username, account_display_name, "
            "caption, hashtags[], mentions[], posted_at, likes_count, comments_count, "
            "views_count, audio_title, is_pinned_post, and comments[]. "
            "For EVERY comment: username, text, time_ago, like_count (integer), is_pinned, replies[]. "
            "Extract ALL comments."
        ),
        source=html_or_text,
        config=graph_config,
    )

    try:
        raw = scraper.run()
    except Exception as e:
        print(f"⚠️  Ошибка ScrapeGraphAI: {e}")
        return {"error": str(e)}
    if isinstance(raw, dict) and "content" in raw and len(raw) == 1:
        return raw["content"]
    return raw


# ── Шаг 3. Сохранение отчёта ────────────────────────────────────────────

def save_report(result: dict, url: str) -> str:
    """Сохраняет результат в history/ как Markdown-отчёт (Obsidian) + JSON."""
    from datetime import datetime

    history_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history")
    os.makedirs(history_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    ts_display = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    shortcode = result.get("shortcode") or url.rstrip("/").split("/")[-1]
    base_name = f"{ts}_{shortcode}"

    # --- JSON ---
    json_path = os.path.join(history_dir, f"{base_name}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # --- Markdown (Obsidian-friendly) ---
    md_path = os.path.join(history_dir, f"{base_name}.md")
    md = []

    def v(key):
        val = result.get(key)
        return val if val is not None else "—"

    username = v("account_username")
    display_name = v("account_display_name")

    # YAML frontmatter для Obsidian
    md.append("---")
    md.append(f"shortcode: {shortcode}")
    md.append(f"account: \"@{username}\"")
    md.append(f"posted_at: \"{v('posted_at')}\"")
    md.append(f"scraped_at: \"{ts_display}\"")
    md.append(f"likes: {v('likes_count')}")
    md.append(f"comments: {v('comments_count')}")
    md.append(f"views: {v('views_count')}")
    md.append(f"source: \"{url}\"")
    md.append("tags: [instagram, scrape]")
    md.append("---")
    md.append("")

    # Заголовок
    md.append(f"# Instagram Reel — @{username}")
    md.append("")
    md.append(f"> Scrape: {ts_display} · [Open in Instagram]({url})")
    md.append("")

    # Метаданные
    md.append("## Метаданные")
    md.append("")
    md.append(f"| Поле | Значение |")
    md.append(f"|------|----------|")
    md.append(f"| **Shortcode** | `{shortcode}` |")
    md.append(f"| **Аккаунт** | @{username} ({display_name}) |")
    md.append(f"| **Дата публикации** | {v('posted_at')} |")
    md.append(f"| **Аудио** | {v('audio_title')} |")
    md.append(f"| **Закреплён** | {v('is_pinned_post')} |")
    md.append(f"| ❤️ **Лайки** | **{v('likes_count')}** |")
    md.append(f"| 💬 **Комментарии** | **{v('comments_count')}** |")
    md.append(f"| 👁 **Просмотры** | {v('views_count')} |")
    md.append("")

    # Подпись
    caption = result.get("caption") or "—"
    md.append("## Подпись")
    md.append("")
    md.append(f"> {caption}")
    md.append("")

    # Хэштеги и упоминания
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

    # Комментарии
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
        time_str = f" · {time_ago}" if time_ago else ""

        md.append(f"### {i}. @{c.get('username', '?')}{pinned}")
        md.append(f"*{time_str.lstrip(' · ')}{likes_str}*")
        md.append("")
        md.append(f"{c.get('text', '')}")
        md.append("")

        replies = c.get("replies") or []
        if replies:
            for j, r in enumerate(replies, 1):
                r_likes = r.get("like_count")
                r_likes_str = f" · ❤️ {r_likes}" if r_likes else ""
                r_time = r.get("time_ago", "")
                r_time_str = f" · {r_time}" if r_time else ""
                md.append(f"  > **↳ @{r.get('username', '?')}** {r_time_str.lstrip(' · ')}{r_likes_str}")
                md.append(f"  > {r.get('text', '')}")
                md.append("")

    # Футер
    md.append("---")
    md.append(f"*Сгенерировано автоматически · {ts_display}*")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    return md_path


# ── main ─────────────────────────────────────────────────────────────────

async def main():
    print("═" * 60)
    print("Шаг 1 [TOOLS]: Playwright — открываем страницу, раскрываем replies")
    print("═" * 60)

    print(f"  Раундов: {MAX_EXPAND_ROUNDS}, пауза: {SCROLL_PAUSE}с, "
          f"стоп после {ZERO_ROUNDS_LIMIT} пустых, headless: {HEADLESS}")

    html = await expand_comments_and_replies(
        url=source_url,
        storage_state=storage_state_path if os.path.exists(storage_state_path) else None,
        headless=HEADLESS,
        max_expand_rounds=MAX_EXPAND_ROUNDS,
        scroll_pause=SCROLL_PAUSE,
        zero_rounds_limit=ZERO_ROUNDS_LIMIT,
    )

    print()
    print("═" * 60)
    print("Шаг 2 [TOOLS]: Regex-парсер — извлекаем комментарии без LLM")
    print("═" * 60)

    clean_text = extract_clean_text(html)
    result = parse_comments_tools(clean_text)

    # Подставляем shortcode из URL если парсер не нашёл
    if not result.get("shortcode"):
        sc_m = re.search(r"/p/([A-Za-z0-9_-]+)", source_url)
        if sc_m:
            result["shortcode"] = sc_m.group(1)

    comments = result.get("comments") or []
    n_comments = len(comments)
    total_replies = sum(len(c.get("replies", [])) for c in comments)
    print(f"  📊 TOOLS-результат: {n_comments} комментариев, {total_replies} ответов")

    # ── Шаг 2.5: Проверка качества TOOLS-парсера → LLM fallback при ошибке ──
    tools_ok = True
    error_ctx = None

    if n_comments < 5:
        tools_ok = False
        error_ctx = _build_error_context(
            phase="tools_extraction",
            problem=f"TOOLS-парсер извлёк только {n_comments} комментариев (ожидалось >5)",
            text_length=len(clean_text),
            text_sample_start=clean_text[:1500],
            text_sample_end=clean_text[-1000:] if len(clean_text) > 2000 else "",
            parsed_comments_sample=[c.get("username") for c in comments[:5]],
        )
    elif all(c.get("like_count", 0) == 0 for c in comments):
        tools_ok = False
        error_ctx = _build_error_context(
            phase="tools_extraction",
            problem="TOOLS-парсер не нашёл like_count ни у одного комментария",
            comments_count=n_comments,
            sample_comments=comments[:3],
        )

    if not tools_ok:
        print()
        print("═" * 60)
        print("Шаг 2.5 [LLM FALLBACK]: TOOLS не справился → отправляю контекст в LLM")
        print("═" * 60)
        print(f"  📤 Error context: {json.dumps(error_ctx, ensure_ascii=False)[:300]}...")

        # Спрашиваем LLM-advisor: что не так и как починить?
        advice = ask_llm_advisor(error_ctx)
        print(f"  💡 LLM-advisor: {advice.get('explanation', '')}")

        # Запускаем LLM-извлечение как fallback
        print("  📡 Запускаю LLM-извлечение...")
        llm_result = _llm_extract_comments(clean_text)

        llm_comments = llm_result.get("comments") or []
        if len(llm_comments) > n_comments:
            print(f"  ✅ LLM нашёл больше: {len(llm_comments)} vs TOOLS {n_comments}")
            result = llm_result
            comments = llm_comments
            n_comments = len(comments)
        else:
            print(f"  ℹ️ LLM не улучшил: {len(llm_comments)} vs TOOLS {n_comments}, оставляю TOOLS")

    print(f"\n📊 Итого комментариев: {n_comments}")

    # ── Шаг 3: Финальная проверка качества ──
    if not isinstance(result, dict):
        result = {"error": "unexpected type", "raw": str(result)[:500]}

    quality = check_extraction_quality(result, expected_comments_hint=None)
    if not quality["ok"]:
        print(f"\n⚠️ Проблемы качества: {', '.join(quality['issues'])}")
        print(f"  💡 LLM-совет: {quality['suggestion']}")
    else:
        print("✅ Качество ОК")

    # Сохраняем отчёт
    report_path = save_report(result, source_url)
    print(f"\n📄 Отчёт сохранён: {report_path}")


if __name__ == "__main__":
    asyncio.run(main())
