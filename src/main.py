from parser.cian_parser import CianPlaywrightParser, ParserConfig


def main():
    config = ParserConfig(
        search_url="https://www.cian.ru/cat.php?currency=2&deal_type=sale&engine_version=2&maxprice=17000000&offer_type=flat&region=1&room3=1",
        max_pages=3,
        max_offers=None,
        download_images=False,
        headless=True,
    )

    parser = CianPlaywrightParser(config)
    offers = parser.run()
    parser.save_json(offers, "data/raw/listings.json")

    print(f"Сохранено объявлений: {len(offers)}")


if __name__ == "__main__":
    main()