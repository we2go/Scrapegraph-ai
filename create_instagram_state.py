import asyncio
from playwright.async_api import async_playwright

STATE_PATH = "instagram_state.json"
LOGIN_URL = "https://www.instagram.com/accounts/login/"


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        print("Открываю страницу логина Instagram...")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")

        print("Войдите вручную в аккаунт в открывшемся окне браузера.")
        print("После успешного входа нажмите Enter в терминале для сохранения cookies.")
        input()

        await context.storage_state(path=STATE_PATH)
        print(f"Сохранено состояние сессии: {STATE_PATH}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
