import json
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import List, Optional
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://www.cian.ru"


@dataclass
class ParserConfig:
    search_url: str
    max_pages: Optional[int] = None
    max_offers: Optional[int] = None
    download_images: bool = False
    headless: bool = True
    timeout_ms: int = 30000
    pause_between_pages: float = 2.0
    pause_between_offers: float = 1.5


@dataclass
class OfferData:
    offer_id: str
    url: str
    price_rub: Optional[int] = None
    price_per_m2_rub: Optional[int] = None
    area_m2: Optional[float] = None
    floor: Optional[int] = None
    floors_total: Optional[int] = None
    address: Optional[str] = None

    living_area_m2: Optional[float] = None
    kitchen_area_m2: Optional[float] = None
    build_year: Optional[int] = None

    rooms: Optional[int] = None
    housing_type: Optional[str] = None
    residential_complex: Optional[str] = None
    year_completion: Optional[int] = None
    building_status: Optional[str] = None

    district: Optional[str] = None
    settlement: Optional[str] = None
    highway: Optional[str] = None
    distance_to_mkad_km: Optional[int] = None

    metro_station: Optional[str] = None
    metro_time_min: Optional[int] = None

    lifts_info: Optional[str] = None
    house_type: Optional[str] = None
    parking: Optional[str] = None
    complex_type: Optional[str] = None

    image_urls: Optional[List[str]] = None
    image_paths: Optional[List[str]] = None


class CianPlaywrightParser:
    def __init__(self, config: ParserConfig):
        self.config = config

    def run(self) -> List[OfferData]:
        results: List[OfferData] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.config.headless,
                slow_mo=0,
            )

            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1400, "height": 900},
                locale="ru-RU",
            )

            page = context.new_page()
            page.set_default_timeout(self.config.timeout_ms)

            offer_urls = self.collect_offer_urls(page)
            print(f"[INFO] Итого ссылок на объявления: {len(offer_urls)}")

            for url in offer_urls:
                offer = self.parse_offer(context, url)
                if offer is not None:
                    results.append(offer)
                time.sleep(self.config.pause_between_offers)

            browser.close()

        return results

    def collect_offer_urls(self, page) -> List[str]:
        urls: List[str] = []
        seen = set()
        page_num = 1
        empty_pages_in_row = 0

        while True:
            if self.config.max_pages is not None and page_num > self.config.max_pages:
                break

            page_url = self._build_page_url(self.config.search_url, page_num)
            print(f"[INFO] Сканирую страницу выдачи: {page_url}")

            try:
                page.goto(page_url, wait_until="domcontentloaded")
                page.wait_for_timeout(2500)

                page.mouse.wheel(0, 2000)
                page.wait_for_timeout(1500)

                html = page.content()
                page_urls = self._extract_offer_urls_from_html(html)

                new_count = 0
                for url in page_urls:
                    if url not in seen:
                        seen.add(url)
                        urls.append(url)
                        new_count += 1

                print(f"[INFO] На странице найдено новых ссылок: {new_count}")
                print(f"[INFO] Найдено ссылок всего: {len(urls)}")

                if self.config.max_offers is not None and len(urls) >= self.config.max_offers:
                    return urls[:self.config.max_offers]

                if new_count == 0:
                    empty_pages_in_row += 1
                else:
                    empty_pages_in_row = 0

                if empty_pages_in_row >= 2:
                    print("[INFO] Новые объявления больше не находятся, останавливаю сбор ссылок.")
                    break

                page_num += 1
                time.sleep(self.config.pause_between_pages)

            except PlaywrightTimeoutError:
                print(f"[ERROR] Таймаут при открытии страницы выдачи: {page_url}")
                empty_pages_in_row += 1
                if empty_pages_in_row >= 2:
                    break
                page_num += 1

            except Exception as e:
                print(f"[ERROR] Не удалось открыть страницу выдачи: {e}")
                empty_pages_in_row += 1
                if empty_pages_in_row >= 2:
                    break
                page_num += 1

        return urls

    def parse_offer(self, context, url: str) -> Optional[OfferData]:
        print(f"[INFO] Открываю объявление: {url}")

        page = context.new_page()
        page.set_default_timeout(self.config.timeout_ms)

        try:
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            os.makedirs("data/raw", exist_ok=True)

            html = page.content()
            text = page.locator("body").inner_text()

            # with open("data/raw/debug_offer.html", "w", encoding="utf-8") as f:
            #     f.write(html)
            #
            # with open("data/raw/debug_offer.txt", "w", encoding="utf-8") as f:
            #     f.write(text)


            offer_id_match = re.search(r"/sale/flat/(\d+)/", url)
            offer_id = offer_id_match.group(1) if offer_id_match else "unknown"

            area = self._extract_area(text)

            price_per_m2 = self._extract_price_per_m2(text)

            price = self._extract_main_price(text, price_per_m2=price_per_m2, area=area)

            if price_per_m2 is None and price is not None and area is not None and area > 0:
                price_per_m2 = round(price / area)

            floor, floors_total = self._extract_floor_info(text)

            address = self._extract_address(text, html)

            living_area = self._extract_living_area(text)
            kitchen_area = self._extract_kitchen_area(text)
            build_year = self._extract_build_year(text)

            rooms, _ = self._extract_rooms_and_title(text)
            housing_type = self._extract_housing_type(text)
            residential_complex = self._extract_residential_complex(text)
            year_completion = self._extract_year_completion(text)
            building_status = self._extract_building_status(text)

            district = self._extract_district(address, text)
            settlement = self._extract_settlement(address, text)
            highway = self._extract_highway(text)
            distance_to_mkad_km = self._extract_distance_to_mkad(text)

            metro_station, metro_time_min = self._extract_metro(text)

            lifts_info = self._extract_lifts_info(text)
            house_type = self._extract_house_type(text)
            parking = self._extract_parking(text)
            complex_type = self._extract_complex_type(text)

            # Фото
            image_urls = self._extract_image_urls(html)

            offer = OfferData(
                offer_id=offer_id,
                url=url,
                price_rub=price,
                price_per_m2_rub=price_per_m2,
                area_m2=area,
                floor=floor,
                floors_total=floors_total,
                address=address,

                rooms=rooms,
                housing_type=housing_type,
                residential_complex=residential_complex,
                year_completion=year_completion,
                building_status=building_status,

                district=district,
                settlement=settlement,
                highway=highway,
                distance_to_mkad_km=distance_to_mkad_km,

                metro_station=metro_station,
                metro_time_min=metro_time_min,

                lifts_info=lifts_info,
                house_type=house_type,
                parking=parking,
                complex_type=complex_type,

                living_area_m2=living_area,
                kitchen_area_m2=kitchen_area,
                build_year=build_year,

                image_urls=image_urls,
                image_paths=[],
            )

            if self.config.download_images and offer.image_urls:
                offer.image_paths = self._download_images(offer.offer_id, offer.image_urls)

            return offer

        except Exception as e:
            print(f"[ERROR] Ошибка при парсинге объявления {url}: {e}")
            return None

        finally:
            page.close()

    def save_json(self, offers: List[OfferData], path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump([asdict(offer) for offer in offers], f, ensure_ascii=False, indent=2)

    def _build_page_url(self, base_url: str, page_num: int) -> str:
        if re.search(r"([?&])p=\d+", base_url):
            return re.sub(r"([?&])p=\d+", rf"\1p={page_num}", base_url)
        separator = "&" if "?" in base_url else "?"
        return f"{base_url}{separator}p={page_num}"

    def _extract_offer_urls_from_html(self, html: str) -> List[str]:
        urls = set(
            re.findall(r'https://www\.cian\.ru/sale/flat/\d+/', html)
        )

        relative_urls = re.findall(r'/sale/flat/\d+/', html)
        for rel in relative_urls:
            urls.add(urljoin(BASE_URL, rel))

        return sorted(urls)

    def _extract_area(self, text: str) -> Optional[float]:
        matches = re.findall(r"([0-9]+(?:[.,][0-9]+)?)\s*м²", text)
        for m in matches:
            value = float(m.replace(",", "."))
            if 10 <= value <= 1000:
                return value
        return None

    def _extract_price_per_m2(self, text: str) -> Optional[int]:
        text = text.replace("\xa0", " ")

        patterns = [
            r"Цена за метр\s*([0-9][0-9\s]{2,})\s*₽\s*/\s*м²",
            r"Цена за метр\s*([0-9][0-9\s]{2,})\s*₽/?м²",
            r"Цена за метр\s*([0-9][0-9\s]{2,})\s*р/?м²",
            r"([0-9][0-9\s]{2,})\s*₽\s*/\s*м²",
            r"([0-9][0-9\s]{2,})\s*₽/?м²",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = re.sub(r"\D", "", match.group(1))
                if value.isdigit():
                    value = int(value)
                    if 10_000 <= value <= 10_000_000:
                        return value

        return None

    def _extract_main_price(
        self,
        text: str,
        price_per_m2: Optional[int] = None,
        area: Optional[float] = None
    ) -> Optional[int]:
        text = text.replace("\xa0", " ")

        candidates = []

        patterns = [
            r"([0-9][0-9\s]{5,})\s*₽",
            r"([0-9][0-9\s]{5,})\s*руб\.?",
            r"([0-9][0-9\s]{5,})\s*р\b",
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text, flags=re.IGNORECASE)
            for m in matches:
                value = re.sub(r"\D", "", m)
                if value.isdigit():
                    num = int(value)
                    if num >= 1_000_000:
                        candidates.append(num)

        candidates = sorted(set(candidates))


        if not candidates:
            return None

        if price_per_m2 is not None and area is not None:
            estimated_price = price_per_m2 * area

            suitable = []
            for x in candidates:
                diff_ratio = abs(x - estimated_price) / estimated_price
                if diff_ratio <= 0.25:
                    suitable.append(x)

            if suitable:
                return min(suitable, key=lambda x: abs(x - estimated_price))

        filtered = [x for x in candidates if x != price_per_m2]

        if filtered:
            return max(filtered)

        return max(candidates)

    def _extract_floor_info(self, text: str) -> tuple[Optional[int], Optional[int]]:
        patterns = [
            r"(\d+)\s*из\s*(\d+)",
            r"Этаж\s*(\d+)\s*из\s*(\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                floor = int(match.group(1))
                floors_total = int(match.group(2))
                if 1 <= floor <= floors_total <= 200:
                    return floor, floors_total

        return None, None

    def _extract_address(self, text: str, html: str) -> Optional[str]:
        patterns = [
            r"(Москва[^.\n]{10,250})",
            r"(Московская область[^.\n]{10,250})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                address = re.sub(r"\s+", " ", match.group(1)).strip()
                address = re.sub(r"\s*[Нн]а карте\s*$", "", address).strip()
                return address

        address = self._extract_address_from_html(html)
        if address:
            address = re.sub(r"\s*[Нн]а карте\s*$", "", address).strip()
        return address

    def _extract_address_from_html(self, html: str) -> Optional[str]:
        candidates = re.findall(r'>([^<>]*Москва[^<>]{0,200})<', html)
        for c in candidates:
            cleaned = re.sub(r"\s+", " ", c).strip()
            cleaned = re.sub(r"\s*[Нн]а карте\s*$", "", cleaned).strip()
            if 8 <= len(cleaned) <= 200:
                return cleaned
        return None

    def _extract_image_urls(self, html: str) -> List[str]:
        urls = set()

        for match in re.finditer(
            r'https://[^"\']+\.(?:jpg|jpeg|png|webp)(?:\?[^"\']*)?',
            html,
            flags=re.IGNORECASE,
        ):
            url = match.group(0)
            if "cian" in url or "cdn" in url:
                urls.add(url)

        return sorted(urls)

    def _download_images(self, offer_id: str, image_urls: List[str]) -> List[str]:
        save_dir = os.path.join("data", "images", offer_id)
        os.makedirs(save_dir, exist_ok=True)

        saved_paths = []
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }

        for i, image_url in enumerate(image_urls, start=1):
            ext_match = re.search(r"\.(jpg|jpeg|png|webp)", image_url, flags=re.IGNORECASE)
            ext = ext_match.group(1).lower() if ext_match else "jpg"
            file_path = os.path.join(save_dir, f"{i}.{ext}")

            try:
                response = requests.get(image_url, headers=headers, timeout=30)
                response.raise_for_status()

                with open(file_path, "wb") as f:
                    f.write(response.content)

                saved_paths.append(file_path)
            except Exception as e:
                print(f"[WARN] Не скачалось изображение {image_url}: {e}")

        return saved_paths

    def _extract_rooms_and_title(self, text: str) -> tuple[Optional[int], Optional[str]]:
        patterns = [
            r"Продается\s+(.+?квартира[^\n]*)",
            r"Продаётся\s+(.+?квартира[^\n]*)",
            r"(\d+-комн\.\s*квартира[^\n]*)",
        ]

        title = None
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                title = re.sub(r"\s+", " ", match.group(1)).strip()
                break

        rooms = None
        if title:
            room_match = re.search(r"(\d+)-комн", title, flags=re.IGNORECASE)
            if room_match:
                rooms = int(room_match.group(1))

        if rooms is None:
            room_match = re.search(r"(\d+)-комн", text, flags=re.IGNORECASE)
            if room_match:
                rooms = int(room_match.group(1))

        return rooms, title

    def _extract_housing_type(self, text: str) -> Optional[str]:
        patterns = [
            r"Тип жилья\s*([^\n]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = re.sub(r"\s+", " ", match.group(1)).strip()
                if 2 <= len(value) <= 100:
                    return value

        if "Новостройка" in text:
            return "Новостройка"

        return None

    def _extract_residential_complex(self, text: str) -> Optional[str]:
        patterns = [
            r"в\s+ЖК\s+«([^»]+)»",
            r"О ЖК\s+«([^»]+)»",
            r'ЖК\s+"([^"]+)"',
            r"ЖК\s+«([^»]+)»",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip()

        return None

    def _extract_year_completion(self, text: str) -> Optional[int]:
        patterns = [
            r"Год сдачи\s*(\d{4})",
            r"срок сдачи:\s*[^,\n]*?(\d{4})",
            r"Сдача в [^\n]*?(\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                year = int(match.group(1))
                if 1900 <= year <= 2100:
                    return year

        return None

    def _extract_building_status(self, text: str) -> Optional[str]:
        patterns = [
            r"Дом\s*([^\n]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = re.sub(r"\s+", " ", match.group(1)).strip()
                if value and len(value) <= 100 and value.lower() != "подписаться на дом":
                    return value

        if "Не сдан" in text:
            return "Не сдан"
        if "Сдан" in text:
            return "Сдан"

        return None

    def _extract_district(self, address: Optional[str], text: str) -> Optional[str]:
        source = address if address else text
        if not source:
            return None

        match = re.search(
            r"(НАО\s*\([^)]+\)|ЮЗАО\s*\([^)]+\)|СЗАО\s*\([^)]+\)|САО\s*\([^)]+\)|СВАО\s*\([^)]+\)|ВАО\s*\([^)]+\)|ЮВАО\s*\([^)]+\)|ЮАО\s*\([^)]+\)|ЗАО\s*\([^)]+\)|ЦАО\s*\([^)]+\))",
            source)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()

        return None

    def _extract_settlement(self, address: Optional[str], text: str) -> Optional[str]:
        if address:
            parts = [p.strip() for p in address.split(",")]
            for part in parts:
                low = part.lower()
                if "деревня" in low or "поселение" in low or "пос." in low or "г." in low:
                    return part

        patterns = [
            r"([А-ЯЁA-Z][а-яёa-z\- ]+\s+деревня)",
            r"([А-ЯЁA-Z][а-яёa-z\- ]+\s+поселение)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return re.sub(r"\s+", " ", match.group(1)).strip()

        return None

    def _extract_highway(self, text: str) -> Optional[str]:
        matches = re.findall(r"([А-ЯЁA-Z][а-яёa-z\-]+(?:\s+[А-ЯЁA-Z]?[а-яёa-z\-]+)*\s+шоссе)", text)
        if matches:
            uniq = []
            for m in matches:
                val = re.sub(r"\s+", " ", m).strip()
                if val not in uniq:
                    uniq.append(val)
            return uniq[0]

        match = re.search(r"ш\.\s*([А-ЯЁA-Z][а-яёa-z\-]+)", text)
        if match:
            return f"{match.group(1).strip()} шоссе"

        return None

    def _extract_distance_to_mkad(self, text: str) -> Optional[int]:
        matches = re.findall(r"(\d+)\s*км\s+от\s+МКАД", text, flags=re.IGNORECASE)
        if matches:
            values = [int(x) for x in matches]
            return min(values)
        return None

    def _extract_metro(self, text: str) -> tuple[Optional[str], Optional[int]]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]

        forbidden = {
            "Боровское шоссе",
            "Киевское шоссе",
            "Сравнить",
            "Пожаловаться",
            "Подробнее",
        }

        for i in range(len(lines) - 1):
            station = lines[i]
            next_line = lines[i + 1]

            if station in forbidden:
                continue

            time_match = re.fullmatch(r"(\d+)\s*мин\.?", next_line, flags=re.IGNORECASE)
            if time_match:
                if 1 <= len(station) <= 50 and not re.search(r"\d", station):
                    return station, int(time_match.group(1))

        return None, None

    def _extract_lifts_info(self, text: str) -> Optional[str]:
        match = re.search(r"Количество лифтов\s*([^\n]+)", text, flags=re.IGNORECASE)
        if match:
            value = re.sub(r"\s+", " ", match.group(1)).strip()
            if value and len(value) <= 100:
                return value
        return None

    def _extract_house_type(self, text: str) -> Optional[str]:
        patterns = [
            r"Тип дома\s*([^\n]+)",
        ]

        matches = re.findall(patterns[0], text, flags=re.IGNORECASE)
        for m in matches:
            value = re.sub(r"\s+", " ", m).strip()
            if value and value.lower() not in {"подписаться на дом"} and len(value) <= 100:
                return value

        return None

    def _extract_parking(self, text: str) -> Optional[str]:
        match = re.search(r"Парковка\s*([^\n]+)", text, flags=re.IGNORECASE)
        if match:
            value = re.sub(r"\s+", " ", match.group(1)).strip()
            if value and len(value) <= 100:
                return value
        return None


    def _extract_complex_type(self, text: str) -> Optional[str]:
        match = re.search(r"Тип комплекса\s*([^\n]+)", text, flags=re.IGNORECASE)
        if match:
            value = re.sub(r"\s+", " ", match.group(1)).strip()
            if value and len(value) <= 100:
                return value
        return None

    def _extract_living_area(self, text: str) -> Optional[float]:
        patterns = [
            r"Жилая площадь\s*([0-9]+(?:[.,][0-9]+)?)\s*м²",
            r"Жилая\s+площадь\s*([0-9]+(?:[.,][0-9]+)?)\s*м²",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = float(match.group(1).replace(",", "."))
                if 5 <= value <= 1000:
                    return value

        return None

    def _extract_kitchen_area(self, text: str) -> Optional[float]:
        patterns = [
            r"Площадь кухни\s*([0-9]+(?:[.,][0-9]+)?)\s*м²",
            r"Кухня\s*([0-9]+(?:[.,][0-9]+)?)\s*м²",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = float(match.group(1).replace(",", "."))
                if 2 <= value <= 500:
                    return value

        return None

    def _extract_build_year(self, text: str) -> Optional[int]:
        patterns = [
            r"Год постройки\s*(\d{4})",
            r"Построен\s+в\s+(\d{4})",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                year = int(match.group(1))
                if 1800 <= year <= 2100:
                    return year

        return None