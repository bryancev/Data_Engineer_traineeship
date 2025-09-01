import time
import scrapy
from scrapy.http import HtmlResponse
from fake_useragent import UserAgent
from urllib.parse import urljoin
from datetime import datetime
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By


class Zolotoy585ParserSpider(scrapy.Spider):
    name = "zolotoy585_parser"
    allowed_domains = ["585zolotoy.ru"]
    start_urls = ["https://585zolotoy.ru"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ua = UserAgent()
        self.search_url = "https://www.585zolotoy.ru/catalog/rings/"
        self.category = "rings"

    def start_requests(self):
        """Сначала пробуем получить все товары через API"""
        page = 1
        all_articles = []

        while True:
            articles, next_page = self.fetch_articles(self.category, page=page)
            if not articles:
                break
            all_articles.extend(articles)
            if not next_page:
                break
            page += 1

        self.logger.info(f"Всего артикулов через API: {len(all_articles)}")

        # Если артикулы есть — парсим их напрямую
        for article in all_articles:
            product_url = urljoin("https://www.585zolotoy.ru/catalog/products/", article)
            yield scrapy.Request(
                url=product_url,
                callback=self.parse_product_page,
                meta={"article": article},
                headers={"User-Agent": self.ua.random},
            )

        # Если API не вернул данных, используем Selenium-scroll
        if not all_articles:
            self.logger.info("API не вернул товары, используем Selenium для скролла")
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument(f"user-agent={self.ua.random}")
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.get(self.search_url)
            self.scroll_page()
            html = self.driver.page_source
            self.driver.quit()
            response = HtmlResponse(url=self.search_url, body=html.encode("utf-8"), encoding="utf-8")
            for item in self.parse(response):
                yield item

    def fetch_articles(self, category, page=1, retries=3):
        """
        Получаем артикулы товаров через API.
        Возвращаем (articles, next_page) или ([], None) при ошибке.
        """
        url = "https://www.585zolotoy.ru/api/v3/products/"
        params = {"category": category, "append": "1", "page": str(page)}
        headers = {"User-Agent": self.ua.random, "accept": "*/*", "x-qa-client-type": "WEB"}

        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)
                if response.ok:
                    data = response.json()
                    results = data.get("results", [])
                    articles = [item.get("article") for item in results if item.get("article")]
                    next_page = data.get("pagination", {}).get("next_page_params")
                    return articles, next_page
                else:
                    self.logger.warning(f"Ошибка статуса {response.status_code} при запросе API")
            except Exception as e:
                self.logger.warning(f"Ошибка запроса API: {e}")
            time.sleep(2)
        return [], None

    def scroll_page(self):
        """Скроллим страницу для подгрузки всех карточек через Selenium"""
        initial_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'a.product-card__name-link'))
        scrolls_count = 5

        for i in range(scrolls_count):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            current_count = len(self.driver.find_elements(By.CSS_SELECTOR, 'a.product-card__name-link'))
            if current_count == initial_count and i > 0:
                break
            initial_count = current_count

        self.logger.info(f"Всего товаров после скролла: {initial_count}")

    def parse(self, response):
        """Парсим карточки товаров после Selenium скролла"""
        products = response.css("a.product-card__name-link")
        self.logger.info(f"Всего товаров после скролла: {len(products)}")

        for product in products:
            link = product.css("::attr(href)").get()
            full_link = urljoin(response.url, link) if link else None

            if link and "/catalog/products/" in link:
                article = link.split("/catalog/products/")[-1].strip("/")
                if full_link:
                    yield scrapy.Request(
                        url=full_link,
                        callback=self.parse_product_page,
                        meta={"article": article},
                        headers={"User-Agent": self.ua.random},
                    )

    def parse_product_page(self, response):
        """Парсим страницу конкретного товара"""
        article = response.meta.get("article", "")
        category, subcategory = self.get_breadcrumbs(response)

        product_data = {
            "category": category,
            "subcategory": subcategory,
            "name": response.css("h1.ui-h1.text-title-2-medium::text").get(),
            "sku": article,
            "price": response.css("strong.product-price__simple-current::text").get(),
            "old_price": response.css("span.product-price__simple-old::text").get(),
            "discount": response.css("span.product-price__simple-discount::text").get(),
            "rating": self.extract_rating(response),
            "reviews": self.extract_reviews(response),
            "parsed_date": datetime.now().strftime("%Y-%m-%d"),
            "product_url": response.url,
        }

        yield product_data

    def get_breadcrumbs(self, response):
        """Получаем категорию и подкатегорию из хлебных крошек."""
        breadcrumbs = response.css(".breadcrumbs a::text, .breadcrumb a::text").getall()

        if breadcrumbs and len(breadcrumbs) >= 3:

            category = breadcrumbs[1]
            subcategory = breadcrumbs[2]
        else:
            category = None
            subcategory = None

        return category, subcategory

    def extract_rating(self, response):
        """Считаем активные звезды рейтинга"""
        rating_elements = response.css("div.active-stars div.icon-svg-box.active")
        return len(rating_elements) if rating_elements else None

    def extract_reviews(self, response):
        """Количество отзывов"""
        reviews_text = response.css("div.text-caption-1-medium.text-text-brand::text").get()
        if reviews_text:
            return reviews_text.split()[0] if reviews_text.strip() else ""
        return ""
