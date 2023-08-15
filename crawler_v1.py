import asyncio
import logging
import re
import sys
from typing import List, Pattern, Set
from urllib.parse import urlparse

import httpx
from selectolax.parser import HTMLParser

logging.basicConfig(
    format="%(levelname)s: %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(), logging.FileHandler(filename="crawler.log")],
)
logger = logging.getLogger(__name__)


class WebScrapper:
    """
    WebScrapper class for crawling and extracting links from web pages.
    """

    def __init__(self, url: str, filter_reg: str, client: httpx.AsyncClient) -> None:
        """
        Initialize the WebScrapper instance.

        Args:
            url (str): The starting URL for crawling.
            filter_reg (str): Regular expression to filter URLs.
            client (httpx.AsyncClient): Asynchronous HTTP client for making requests.
        """
        self.url = url
        self.url_parsed = urlparse(url)
        self.client = client
        self.filtered_url = set()
        self.visited_url = set()
        self.filter_reg: Pattern = re.compile(filter_reg)
        self.amount = 0

    async def _http_get(self, url: str) -> HTMLParser:
        """
        Asynchronously fetch and parse the HTML content of a URL.

        Args:
            url (str): The URL to fetch content from.

        Returns:
            HTMLParser: Parsed HTML content.
        """
        headers = {"user-agent": "Magic Browser"}
        resp = await self.client.get(url, headers=headers, follow_redirects=False)
        resp.raise_for_status()
        return HTMLParser(resp.content)

    def harvest(self, html: HTMLParser, filter: Pattern) -> None:
        """
        Extract links from parsed HTML content and add to filtered URLs.

        Args:
            html (HTMLParser): Parsed HTML content.
            filter (Pattern): Regular expression pattern for filtering links.
        """
        nodes = html.css("a[href]")
        for node in nodes:
            url: str = node.attributes.get("href")  # type: ignore
            if re.match(filter, url):
                logger.info(url)
                self.filtered_url.add(url)

    async def _fetch_new_site(self, url: str) -> List[str]:
        """
        Fetch and process a new URL for crawling.

        Args:
            url (str): The URL to fetch and process.

        Returns:
            List[str]: List of links found on the fetched page.
        """
        if url in self.visited_url:
            logger.warn(f"Revisiting: {url} {self.visited_url=}")
            raise httpx.HTTPError("Revisit")
        elif urlparse(url).path.endswith(
            (".mp4", ".jpg", ".jpeg", ".mov", ".mkv", ".gif", ".gifv", ".png", ".webp")
        ):
            raise httpx.InvalidURL(url)
        self.visited_url.add(url)
        self.amount += 1
        logger.debug(f"crawling {url}")
        html = await self._http_get(url)
        self.harvest(html, self.filter_reg)
        nodes = html.css(
            f'a[href^="{self.url_parsed.scheme + "://" + self.url_parsed.netloc}"]'
        )
        return [link.attributes.get("href") for link in nodes]  # type: ignore

    async def crawl(self, url) -> None:
        """
        Recursively crawl URLs and extract links.

        Args:
            url (str): The URL to start crawling from.
        """
        url = url.rstrip("/")
        if url not in self.visited_url:
            try:
                links = await self._fetch_new_site(url)
            except httpx.HTTPError as e:
                logger.error(f"{e.request.url}", exc_info=True)
                return
            except httpx.InvalidURL as url:
                logger.warning(f"Skipping {url}")
                return
            tasks = [self.crawl(link) for link in links if link.startswith("http")]
            await asyncio.gather(*tasks)

    async def __call__(self) -> Set[str]:
        """
        Start crawling from the initial URL and return the filtered URLs.

        Returns:
            Set[str]: Set of filtered URLs.
        """
        await self.crawl(self.url)
        logger.info(f"{self.amount} pages crawled")
        return self.filtered_url


async def main():
    """
    Example Case to initiate crawling.
    """
    try:
        sys.argv[3]
    except IndexError:
        print(f"Usage: {sys.argv[0]} url regex max_connection")
        return
    except ValueError:
        print("Error: max_connection should be an integer")
        return
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(5.0, pool=None),
        limits=httpx.Limits(max_connections=int(sys.argv[3])),
    ) as client:
        drop = WebScrapper(client=client, url=sys.argv[1], filter_reg=sys.argv[2])
        try:
            await drop()
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.warning("User tried to exit")
        finally:
            logger.info(
                f"{len(drop.filtered_url)} links harvested from {drop.amount} pages"
            )
            logger.info("Writing links to file")
            with open(drop.url_parsed.netloc, "w") as file:
                file.writelines(url + "\n" for url in drop.filtered_url)


if __name__ == "__main__":
    asyncio.run(main())
