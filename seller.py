import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон

    Получает список товаров с интернет магазина Озон клиента

    Args:
        last_id (str): Идентификатор последнего значения на странице.
                       Оставьте это поле пустым при выполнении первого запроса.
                       Чтобы получить следующие значения,
                       укажите last_id из ответа предыдущего запроса.

        client_id (str): Идентификатор клиента.

        seller_token (str): API-ключ

    Returns:
        Словарь "items": список из словарей:
                                "product_id": идентификатор продукта
                                "offer_id": артикул товара
                "total": колличество товаров
                "last_id": Идентификатор последнего значения на странице

    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Args:
        client_id (str): Идентификатор клиента
        seller_token (str): API-ключ

    Returns:
        Список артикулов
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров

    Изменить цены продавца на сайте Озон товаров часов

    Args:
        prices: (list): Список из словарей:
                        "auto_action_enabled": "UNKNOWN"
                        "currency_code": Валюта ("RUB")
                        "offer_id": Артикул (из словаря watch_remnants)
                        "old_price": Старая цена ("0")
                        "price": Цена (из словаря watch_remnants)
        client_id (str): Идентификатор клиента
        seller_token (str): API-ключ

    Returns: Список из словарей:
        "product_id": Идентификатор товара (1386)
        "offer_id": Артикул товара ("PH8865")
        "updated": Произошло ли изменение (true),
        "errors": список из словарей ошибок, если есть
            "code" (int): Код ошибки
            "details" (list): Дополнительная информация об ошибке.
            "message" (str): Описание ошибки.
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

    изменить информацию о количестве товара часов в наличии 
    продавца на сайте Озон

    Args:
        stocks: (list): Список из словарей:
                        "offer_id": Артикул
                        "stock": Колличиство
        client_id (str): Идентификатор клиента
        seller_token (str): API-ключ

    Returns: Список из словарей:
        "product_id": Идентификатор товара (1386)
        "offer_id": Артикул товара ("PH8865")
        "updated": Произошло ли изменение (true),
        "errors": список из словарей ошибок, если есть
            "code" (int): Код ошибки
            "details" (list): Дополнительная информация об ошибке.
            "message" (str): Описание ошибки.
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio
    
    Скачивает exel файл остатков часов casio с сайта
    https://timeworld.ru в zip архиве 
    и создает список остатков часов.
    
    Returns:
        Список словарей:
            'Код': Код товара (68122), 
            'Наименование товара': Наименование ('BA-110-4A1'), 
            'Изображение': ('Показать'), 
            'Цена': Цена товара ("16'590.00 руб."), 
            'Количество': Количество товара ('>10'), 
            'Заказ': ''
    """

    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """ Создает Список склада

    Создается Список артикла и колличеста из списка артикулов и остатков часв

    Если в остатках часов количество ">10" то заноситься количество 100
    Если остатки равны "1" или артикла нет в остатках заоситься количество 0

    Args:
        watch_remnants (list): список остатков часов
        offer_ids (list): Список артикулов

    Returns:
        Список из словарей:
            "offer_id": Артикул
            "stock": Колличиство
    """

    # Уберем то, что не загружено в seller

    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))

    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Выставляются цены на часы по их артиклу

    Args:
        watch_remnants (list): Список часов
        offer_ids (list): Список артикулов

    Returns:
        Список словаря:
            "auto_action_enabled": "UNKNOWN"
            "currency_code": Валюта ("RUB")
            "offer_id": Артикул (из словаря watch_remnants)
            "old_price": Старая цена ("0")
            "price": Цена (из словаря watch_remnants)
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену. 

    Удаляет из строки цена все лишние символы,
    возвращает целое число строкового типа

    Пример: 5'990.00 руб. -> 5990"""

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

    Пример: 
    >>> print(divide(range(1, 16), 6))
    [[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12], [13, 14, 15]]

    Args:
        lst (list): Список элементов
        n (int): Количество элементов в списке

    Returns:
        Список из списка по n элементов
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
