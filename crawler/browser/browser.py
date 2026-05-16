#!/user/bin/env python
"""
@Time   : 2022-01-23 15:24
@Author : LFY
@File   : browser.py
"""

# here put the import lib
import asyncio
import json
import logging

from playwright.async_api import Browser, BrowserContext, Cookie, async_playwright

from config.crawl_config import CHROME_BROWSER_PATH, crawler_config


class BrowserHandler:
    """ """

    CHROME_ARGS = [
        "--enable-features=NetworkService",
        "--disable-setuid-sandbox",
        "--disable-popup-blocking",
        "--disable-webgl",
        "--disable-images",
        "--no-sandbox",
        "--allow-running-insecure-content",
        "--disable-web-security",
        "--disable-xss-auditor",
        "--disable-gpu",
        "--no-recovery-component",
        "--ignore-certificate-errors",
    ]

    @classmethod
    async def create(cls):
        self = BrowserHandler()
        await self._init_browser()
        await asyncio.sleep(0.5)
        await self._init_browser_context()
        return self

    def __init__(self):
        self.browser: Browser | None = None
        self.p = None
        self.browser_context: BrowserContext | None = None

    @staticmethod
    def _build_extra_headers():
        headers = dict(crawler_config.EXTRA_HEADER) if crawler_config.EXTRA_HEADER else {}
        if crawler_config.COOKIE:
            cookie_header = "; ".join(f"{key}={value}" for key, value in crawler_config.COOKIE.items())
            if cookie_header:
                headers["Cookie"] = cookie_header
        return headers if headers else None

    async def _init_browser(self):
        """
        Init browser context

        :return:
        """
        self.p = await async_playwright().start()
        logging.debug("[+] Open browser")

        proxy = {"server": crawler_config.PROXY} if crawler_config.PROXY else None
        self.browser = await self.p.chromium.launch(
            headless=crawler_config.HEADLESS_MODE,
            args=self.CHROME_ARGS,
            executable_path=CHROME_BROWSER_PATH,
            proxy=proxy,
        )

    async def _init_browser_context(self):
        self.browser_context = await self.browser.new_context(
            ignore_https_errors=True,
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            locale="en-US",
            storage_state=crawler_config.COOKIE_PATH,
            extra_http_headers=self._build_extra_headers(),
        )
        # Warm up authentication: navigate each origin once (without any
        # route/request interceptors) so the SPA can run its token-refresh
        # flow freely and store a fresh access token into localStorage.
        # All pages created afterwards from the same context will share
        # that fresh localStorage state.
        if crawler_config.COOKIE_PATH:
            await self._warm_up_auth_state()

    async def _warm_up_auth_state(self):
        """Navigate each origin from the cookie file once without interceptors.

        Playwright's storage_state injects localStorage at context-creation time,
        but the stored access token may already be expired (Memos tokens live only
        ~15 minutes).  By navigating to the origin in a plain page (no route/
        request hooks), the SPA is free to call its token-refresh endpoint using
        the long-lived memos_refresh cookie and store a brand-new access token
        back into localStorage.  Because all pages in a Playwright context share
        the same per-origin localStorage, every subsequent crawl page will start
        with a valid token without needing to refresh again.
        """
        try:
            with open(crawler_config.COOKIE_PATH) as f:
                data = json.load(f)
        except Exception as e:
            logging.debug(f"[-] Auth warm-up: failed to read cookie file: {repr(e)}")
            return

        origins = [
            entry.get("origin")
            for entry in (data.get("origins") or [])
            if entry.get("origin")
        ]
        # Fallback: derive origin from cookie domain when no origins section exists
        if not origins:
            for cookie in data.get("cookies") or []:
                domain = cookie.get("domain", "").lstrip(".")
                if domain:
                    origins.append(f"http://{domain}")
                    break

        for origin in origins:
            page = None
            try:
                page = await self.browser_context.new_page()
                logging.debug(f"[+] Auth warm-up: navigating to {origin}")
                await page.goto(origin, timeout=15000)
                # Wait until the SPA's token-refresh network traffic settles.
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except Exception:
                    pass  # timeout is acceptable – proceed with whatever state we have
                logging.debug("[+] Auth warm-up: done")
            except Exception as e:
                logging.debug(f"[-] Auth warm-up failed for {origin}: {repr(e)}")
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:
                        pass

    async def refresh_context(
        self,
    ):
        cookies: list[Cookie] = await self.browser_context.cookies()
        new_cookies: list = []
        for ck in cookies:
            new_ck = {
                "name": ck.get("name"),
                "value": ck.get("value"),
                "domain": ck.get("domain"),
                "path": ck.get("path"),
                "expires": ck.get("expires"),
                "httpOnly": ck.get("httpOnly"),
                "secure": ck.get("secure"),
                "sameSite": ck.get("sameSite"),
            }
            new_cookies.append(new_ck)
        self.browser_context = await self.browser.new_context(
            ignore_https_errors=True,
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            locale="en-US",
            storage_state=crawler_config.COOKIE_PATH,
            extra_http_headers=self._build_extra_headers(),
        )
        await self.browser_context.add_cookies(new_cookies)

    async def safe_close_browser(self):
        try:
            await self.browser.close()
            await self.p.stop()
        except Exception as e:
            logging.error("[!] Critical Error at closing browser:", repr(e))
