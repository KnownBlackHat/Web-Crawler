import asyncio
import logging
import re
import sys
from datetime import datetime, timezone
from time import perf_counter
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
        max_retry: int,
    ) -> None:
        self.queue = asyncio.Queue()
        self.num_worker = worker
        self.done = set()
        self.seen = set()
        self.filtered_url = set()
        self.start_url = {url}
        self.client = client
        self.max_retry = max_retry
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
        self.seen.update(new)

        for url in new:
            await self.queue.put((url, self.max_retry))

    def harvest(self, response: httpx.Response, regex: Pattern) -> None:
        parsed_html = HTMLParser(response.content)
        nodes = parsed_html.css("a[href]")
        for node in nodes:
            url: str = node.attributes.get("href")  # type: ignore
            if not url:
                continue
            elif re.match(regex, url):
                self.filtered_url.add(url)

    async def _fetch_new_site(self, url: str) -> Set[str]:
        if urlparse(url).path.endswith(
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
            f'a[href^="{self.url_parsed.scheme + "://" + self.url_parsed.netloc}"], a[href^="/"]'
        )
        return {
            link.attributes.get("href").rstrip("/")
            if not link.attributes.get("href").startswith("/")  # type: ignore
            else f'{self.url_parsed.scheme}://{self.url_parsed.netloc}/{link.attributes.get("href")}'
            for link in nodes
        }

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
        url, retry = await self.queue.get()
        try:
            await self.crawl(url)
        except httpx.HTTPStatusError as err:
            if retry > self.max_retry:
                logger.critical(f"Failed to process {url}")
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

            logger.info(
                f"Got {err.response.status_code} at {url} Retry Attempt: {retry}"
            )
            await self.queue.put((url, retry - 1))
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

    async with httpx.AsyncClient(timeout=httpx.Timeout(None)) as client:
        start = perf_counter()
        crawler = WebCrawler(
            worker=int(sys.argv[3]),
            url=sys.argv[1],
            client=client,
            regex=sys.argv[2],
            max_retry=5,
        )
        try:
            await crawler()
        finally:
            print()
            print(f"Crawled: {len(crawler.done)}")
            print(f"Harvested: {len(crawler.filtered_url)}")
            print(f"Time Taken: {perf_counter() - start:.2f} secs")
            with open(crawler.url_parsed.netloc, "w") as file:
                file.writelines(line + "\n" for line in crawler.filtered_url)


if __name__ == "__main__":
    asyncio.run(main())
