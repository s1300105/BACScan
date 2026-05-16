#!/user/bin/env python
"""
@Time   : 2022-01-23 15:28
@Author : LFY
@File   : task.py
"""

import asyncio
import json

from config import *

# here put the import lib
from crawler.browser.browser import BrowserHandler
from crawler.browser.js.js_crawlergo import *
from crawler.crawl.modules.console import console_handler
from crawler.crawl.modules.event import trigger_events
from crawler.crawl.modules.form import fill_handler, form_submit
from crawler.crawl.modules.link import *
from crawler.crawl.modules.route import request_handler, route_handler
from crawler.models.nav_graph import NavigationGraph
from crawler.models.request import Request
from crawler.utils import *


async def _inject_local_storage(page, cookie_path):
    """Inject localStorage entries from auth file via add_init_script.

    Playwright's storage_state may not reliably restore localStorage for SPAs
    that check it before the first navigation completes.  Using add_init_script
    guarantees the entries exist before any page JavaScript runs.
    """
    if not cookie_path:
        return
    try:
        with open(cookie_path) as f:
            data = json.load(f)
        scripts = []
        for origin_entry in (data.get("origins") or []):
            for item in (origin_entry.get("localStorage") or []):
                name = item.get("name")
                value = item.get("value")
                if name is not None and value is not None:
                    scripts.append(
                        f"window.localStorage.setItem({json.dumps(str(name))}, {json.dumps(str(value))});"
                    )
        if scripts:
            await page.add_init_script(script="\n".join(scripts))
    except Exception as e:
        logging.debug(f"[-] Failed to inject localStorage from {cookie_path}: {repr(e)}")


class Crawler:
    @classmethod
    async def create(cls):
        self = Crawler()
        self.browser_handler = await BrowserHandler.create()
        return self

    def __init__(self):
        self.browser_handler: Optional[BrowserHandler] = None

    @timeout(seconds=20)
    async def get_content(self, req: Request):
        response = None
        page = await PageHandler(self.browser_handler.browser_context).get_new_page()
        # page.on("response", lambda response: vuln_response_handler(response))
        headers = req.headers or {}
        if headers:
            sanitized = {}
            for k, v in headers.items():
                if str(k).lower() == "content-length":
                    continue
                if v is None:
                    continue
                sanitized[k] = v
            headers = sanitized

        if req.method in ["POST"]:
            data = init_post_data(req)
            response = await page.request.post(req.url, headers=headers, data=data)

        elif req.method == "GET":
            response = await page.request.get(req.url, headers=headers)

        elif req.method == "PATCH":
            data = init_post_data(req)
            response = await page.request.patch(req.url, headers=headers, data=data)

        elif req.method == "DELETE":
            data = init_post_data(req)
            response = await page.request.delete(req.url, headers=headers, data=data)

        elif req.method == "PUT":
            data = init_post_data(req)
            response = await page.request.put(req.url, headers=headers, data=data)

        elif req.method == "FETCH":
            response = await page.request.fetch(req.url, headers=headers, data=init_post_data(req))

        status_code = response.status
        if any(status_code == i for i in [404, 400]):
            logging.debug(f"[+] Non-200 status {status_code} for {req.url}")
        try:
            html_content = await response.text()
        except Exception as e:
            logging.debug(f"[-] Failed to read response body: {repr(e)}")
            return "ERRURL"
        return html_content

    @timeout(seconds=200)
    async def crawl_pages(self, req: Request, navgraph: NavigationGraph) -> set[Request]:
        """
        Open a page and crawl it

        :return:
        """
        collected_urls: set[Request] = set()

        """
        Step0: Get a new page
        """
        page = await PageHandler(self.browser_handler.browser_context).get_new_page()

        """
        Step1: Inject JS into page and init page's env
        """
        # Inject JS into page
        await page.add_init_script(script=TabInitJS)

        # dismiss dialog
        page.on("dialog", Crawler.dialog_handler)
        page.on("popup", lambda popup: asyncio.create_task(Crawler.popup_handler(popup, page, collected_urls)))
        # close 401
        # TODO

        # set default page run timeout and dom content loaded timeout
        page.set_default_navigation_timeout(crawler_config.TAB_RUN_TIMEOUT)
        page.set_default_timeout(crawler_config.DOM_LOADED_TIMEOUT)

        """
        Step2: Init JS-Python communication bridge
        """
        page.on("console", lambda console: console_handler(console, page, collected_urls))

        """
        Step3: Register callback to collect all static links
        """
        page.on(
            "response",
            lambda response: response_handler(
                response, page, collected_urls, navgraph, req.url, self.browser_handler.browser_context
            ),
        )

        """
        Step4: Init request-hook module
        """
        page.on("request", lambda request: request_handler(request, collected_urls))
        await page.route("**", lambda route: route_handler(route, req, collected_urls, navgraph))

        """
        Step5: Go to target URL
        """
        if not await PageHandler.safe_goto(page, req.url):
            return collected_urls

        # Wait for the SPA to complete its auth/data-fetch flow (token refresh,
        # initial API calls, etc.) before we start collecting links.
        # networkidle resolves once there have been no new network requests for
        # 500 ms, which is enough for a Memos-style refresh-token flow.
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass  # timeout is fine – proceed with whatever loaded

        html_content = await page.content()
        req.set_response(html_content)

        """
        Step6: Set observer to record dom changed
        """
        try:
            await PageHandler.safe_evaluate(page, ObserverJS)
        except Exception as e:
            logging.debug(f"[-] Error at set observer: {repr(e)}")

        """
        Step7: Collect static url from current page
        """
        await collect_href_links(page, collected_urls)
        await collect_obj_links(page, collected_urls)
        await collect_comment_links(page, collected_urls)

        """
        Step8: Fill forms
        """
        logging.debug("[+] Start fill forms")
        await asyncio.sleep(0.5)
        await fill_handler(["input", "textarea", "select option:first-child"], page)
        await asyncio.sleep(0.5)

        """
        Step9: Submit forms
        """
        logging.debug("[+] Start submit forms")
        # Three ways to submit forms
        await form_submit(page)

        """
        Step10: Trigger events(Dom0 & Dom2 & HTML Events)
        """
        logging.debug("[+] Start trigger events")
        trigger_interval = crawler_config.EVENT_TRIGGER_INTERVAL
        await trigger_events(page, trigger_interval)

        """
        Step7-redo: Collect static url from current page
        """
        await collect_href_links(page, collected_urls)
        await collect_obj_links(page, collected_urls)
        await collect_comment_links(page, collected_urls)
        """
        Step11: Remove DOM Listener
        """
        # logging.debug("[*] Set dom change observer")
        try:
            await PageHandler.safe_evaluate(page, RemoveDOMListenerJS)
        except Exception as e:
            logging.debug(f"[-] Error at remove DOM Listener: {repr(e)}")

        logging.debug(f"[+] Closing page {req.url}")
        await page.close()

        return collected_urls

    @staticmethod
    async def dialog_handler(dialog: playwright.async_api.Dialog):
        """
        Dismiss alert, confirm, prompt, or onbeforeunload

        :param dialog:
        :return:
        """
        try:
            await dialog.dismiss()
        except Exception as e:
            logging.debug(f"[-] Error at dismiss dialog: {repr(e)}")

    @staticmethod
    async def popup_handler(
        popup: playwright.async_api.Page, opener_page: playwright.async_api.Page, collected_urls: set[Request]
    ):
        try:
            try:
                await popup.wait_for_load_state("domcontentloaded", timeout=2000)
            except Exception:
                await asyncio.sleep(0.2)

            popup_url = popup.url
            if popup_url and popup_url != "about:blank":
                collected_urls.add(Request(popup_url, from_url=opener_page.url))
            logging.debug(f"[+] Closing popup {popup_url} from {opener_page.url}")
        except Exception as e:
            logging.debug(f"[-] Error at popup handler: {repr(e)}")
        finally:
            try:
                await popup.close()
            except Exception:
                pass
