import asyncio
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Pattern, Set
from urllib.parse import urlparse

import httpx
from selectolax.parser import HTMLParser

logging.basicConfig(
    format="%(levelname)s: %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(), logging.FileHandler(filename="crawler.log")],
)
logger = logging.getLogger(__name__)


class WebCrawler:
    def __init__(
        self,
        *,
        worker: int,
        url: str,
        client: httpx.AsyncClient,
        regex: str,
    ) -> None:
        self.queue = asyncio.Queue()
        self.num_worker = worker
        self.done = set()
        self.seen = set()
        self.filtered_url = set()
        self.start_url = {url}
        self.client = client
        self.reg = re.compile(regex)
        self.url_parsed = urlparse(url)
        self.rate_limit = None

    async def __call__(self) -> None:
        await self.on_found_links(self.start_url)

        workers = [asyncio.create_task(self.worker()) for _ in range(self.num_worker)]

        await self.queue.join()

        for worker in workers:
            worker.cancel()

    async def on_found_links(self, urls: set[str]) -> None:
        new = urls - self.seen
        self.start_url.update(new)

        for url in new:
            await self.queue.put(url)

    def harvest(self, response: httpx.Response, regex: Pattern) -> None:
        parsed_html = HTMLParser(response.content)
        nodes = parsed_html.css("a[href]")
        for node in nodes:
            url: str = node.attributes.get("href")  # type: ignore
            if not url:
                continue
            elif re.match(regex, url):
                logger.info(url)
                self.filtered_url.add(url)

    async def _fetch_new_site(self, url: str) -> Set[str]:
        if url in self.seen:
            logger.warn(f"Revisiting: {url} {self.seen=}")
            raise httpx.HTTPError("Revisit")
        elif urlparse(url).path.endswith(
            (".mp4", ".jpg", ".jpeg", ".mov", ".mkv", ".gif", ".gifv", ".png", ".webp")
        ):
            raise httpx.InvalidURL(url)
        logger.debug(f"crawling {url}")
        headers = {"user-agent": "Cursed Browser"}
        resp = await self.client.get(url, headers=headers, follow_redirects=True)
        resp.raise_for_status()
        self.harvest(resp, self.reg)
        html = HTMLParser(resp.content)
        nodes = html.css(
            f'a[href^="{self.url_parsed.scheme + "://" + self.url_parsed.netloc}"]'
        )
        return {link.attributes.get("href").rstrip("/") for link in nodes}  # type: ignore

    async def crawl(self, url: str) -> None:
        """Crawls the page"""
        await asyncio.sleep(0.1)  # rate limit fix

        found_links = await self._fetch_new_site(url)

        await self.on_found_links(found_links)

        self.done.add(url)

    async def worker(self) -> None:
        while True:
            try:
                await self.process_one()
            except asyncio.CancelledError:
                return

    async def process_one(self) -> None:
        if self.rate_limit:
            logger.warning(f"Found RateLimit Lock, Sleeping for {self.rate_limit + 0}")
            await asyncio.sleep(self.rate_limit + 1)
            logger.info("Rate Limit Ended")
            self.rate_limit = None
        url = await self.queue.get()
        try:
            await self.crawl(url)
        except httpx.HTTPStatusError as err:
            if err.response.status_code == 429:
                rate_limit = err.response.headers.get("Retry-After")
                try:
                    self.rate_limit = float(rate_limit)
                except ValueError:
                    parsed_datetime = datetime.strptime(
                        rate_limit, "%a, %d %b %Y %H:%M:%S %z"
                    ).replace(tzinfo=timezone.utc)
                    current_datetime = datetime.now(timezone.utc)
                    time_delta = parsed_datetime - current_datetime
                    self.rate_limit = time_delta.total_seconds()

                logger.info(f"Got 429 at {url} putting back to queue")
                await self.queue.put(url)
        except Exception:
            logger.error("Error at process one", exc_info=True)
        finally:
            self.queue.task_done()


async def main():
    try:
        sys.argv[3]
    except IndexError:
        print(
            f"usage: {sys.argv[0]} url_to_crawl regex_4_link_harvester number_of_worker"
        )
        return

    async with httpx.AsyncClient() as client:
        crawler = WebCrawler(
            worker=int(sys.argv[3]), url=sys.argv[1], client=client, regex=sys.argv[2]
        )
        try:
            await crawler()
        finally:
            with open(crawler.url_parsed.netloc, "w") as file:
                file.writelines(line + "\n" for line in crawler.filtered_url)


if __name__ == "__main__":
    asyncio.run(main())
