import asyncio
import csv
from dataclasses import dataclass, fields
from time import time
from urllib.parse import urljoin
import aiofiles
import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://quotes.toscrape.com/"


@dataclass
class Quote:
    text: str
    author: str
    tags: list[str]

    def to_dict(self):
        return {
            field.name: getattr(self, field.name)
            for field in fields(self)
        }


async def parse_url_for_quote(
        url: str,
        session: aiohttp.ClientSession
) -> list[Quote] | None:
    async with session.get(url) as response:
        if response.status != 200:
            return None
        soups = BeautifulSoup(await response.text(), "html.parser")
        quote_list = [
            Quote(
                text=soup.select_one(".text").text,
                author=soup.select_one(".author").text,
                tags=[tag.text for tag in soup.select(".tag")],
            ) for soup in soups.select(".quote")
        ]
        return quote_list


async def parse_all_quotes() -> list[Quote]:
    page = 1
    all_quotes = []
    async with aiohttp.ClientSession() as session:
        while quote_list := await parse_url_for_quote(
                urljoin(BASE_URL, f"/page/{page}/"),
                session
        ):
            page += 1
            all_quotes.extend(quote_list)
    return all_quotes


async def create_and_fill_csv(
        quote_list: list[Quote],
        output_csv_path: str
) -> None:
    async with aiofiles.open(output_csv_path, "w", newline='') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            [field.name for field in fields(Quote)]
        )
        await writer.writeheader()
        await asyncio.gather(*[writer.writerow(quote.to_dict()) for quote in quote_list])


async def main(output_csv_path: str) -> None:
    await create_and_fill_csv(await parse_all_quotes(), output_csv_path)


if __name__ == "__main__":
    start = time()
    asyncio.run(main("quotes.csv"))
    print(time() - start)
