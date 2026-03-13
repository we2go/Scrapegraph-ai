import asyncio
import os
import re

from dotenv import load_dotenv
from playwright.async_api import async_playwright
from undetected_playwright import Malenia

load_dotenv()

STATE_PATH = os.getenv("INSTAGRAM_STORAGE_STATE", "instagram_state.json")
LOGIN_URL = "https://www.instagram.com/accounts/login/"


async def main():
    username = os.getenv("INSTAGRAM_USERNAME")
    password = os.getenv("INSTAGRAM_PASSWORD")
    if not username or not password:
        print("Ошибка: INSTAGRAM_USERNAME и INSTAGRAM_PASSWORD должны быть заданы в .env")
        exit(1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        await Malenia.apply_stealth(context)
        page = await context.new_page()

        print("Открываю страницу логина Instagram...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await asyncio.sleep(5)

        # Закрываем куки-модалку — пробуем все варианты
        for txt in [
            "Разрешить все cookie",
            "Allow all cookies",
            "Accept all cookies",
            "Принять все",
            "Отклонить необязательные файлы cookie",
            "Decline optional cookies",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(re.escape(txt), re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    print(f"  Куки-модалка закрыта: {txt}")
                    await asyncio.sleep(3)
                    break
            except Exception:
                pass

        # Также пробуем кнопку через текст (Instagram иногда прячет role)
        for txt in ["cookie", "Cookie", "Разрешить", "Allow"]:
            try:
                btns = page.locator(f"button:has-text('{txt}')")
                if await btns.count() > 0:
                    await btns.first.click(timeout=5000)
                    print(f"  Куки-кнопка (text): {txt}")
                    await asyncio.sleep(3)
                    break
            except Exception:
                pass

        # Ждём загрузку страницы после куки-диалога
        await asyncio.sleep(3)
        print(f"  URL: {page.url}")
        print(f"  Title: {await page.title()}")

        # Если нас перенаправило — вернёмся на страницу логина
        if "/accounts/login" not in page.url:
            print(f"  Перенаправлено, возвращаемся на логин...")
            await page.goto(LOGIN_URL, wait_until="domcontentloaded")
            await asyncio.sleep(5)

        # Пробуем найти форму — несколько вариантов селекторов
        print(f"Вхожу как {username}...")
        login_selectors = [
            "input[name='email']",
            "input[name='username']",
            "input[type='text']",
        ]

        username_input = None
        for selector in login_selectors:
            try:
                el = page.locator(selector).first
                if await el.count() > 0 and await el.is_visible():
                    username_input = el
                    print(f"  Поле логина найдено: {selector}")
                    break
            except Exception:
                pass

        if username_input is None:
            print("⚠️  Форма логина не найдена автоматически.")
            print("Войдите вручную в открытом браузере и нажмите Enter.")
            input()
            await context.storage_state(path=STATE_PATH)
            print(f"Сохранено: {STATE_PATH}")
            await browser.close()
            return

        await username_input.click()
        await username_input.fill(username)
        await asyncio.sleep(0.5)

        # Ищем поле пароля
        password_input = page.locator("input[name='pass'], input[name='password'], input[type='password']").first
        await password_input.click()
        await password_input.fill(password)
        await asyncio.sleep(0.5)

        # Отправляем форму через Enter
        await password_input.press("Enter")
        print("Enter нажат, ожидаю результат входа...")

        # Ждём навигации после входа
        try:
            await page.wait_for_url("**/accounts/onetap/**", timeout=30000)
            print("Вход выполнен — страница onetap.")
        except Exception:
            # Может быть другой redirect или challenge
            await asyncio.sleep(10)
            print(f"Текущий URL: {page.url}")

        # Закрываем «Save login info» / «Не сейчас» если появится
        for txt in ["Not Now", "Не сейчас", "Save Info", "Сохранить"]:
            try:
                btn = page.get_by_role("button", name=re.compile(re.escape(txt), re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    print(f"  Закрыта модалка: {txt}")
                    await asyncio.sleep(2)
                    break
            except Exception:
                pass

        # Закрываем «Turn on notifications» / «Не сейчас»
        await asyncio.sleep(2)
        for txt in ["Not Now", "Не сейчас"]:
            try:
                btn = page.get_by_role("button", name=re.compile(re.escape(txt), re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    print(f"  Закрыта модалка уведомлений: {txt}")
                    await asyncio.sleep(2)
                    break
            except Exception:
                pass

        await context.storage_state(path=STATE_PATH)
        print(f"Сохранено состояние сессии: {STATE_PATH}")

        # Проверим что sessionid есть
        cookies = await context.cookies()
        session_cookie = [c for c in cookies if c["name"] == "sessionid"]
        if session_cookie:
            print("✅ sessionid найден — авторизация успешна!")
        else:
            print("⚠️  sessionid не найден — возможно нужно пройти challenge вручную.")
            print("Если нужно — завершите вход в браузере и нажмите Enter.")
            input()
            await context.storage_state(path=STATE_PATH)
            print(f"Пересохранено: {STATE_PATH}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
