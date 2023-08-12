import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров магазина Яндекс

    Получает список товаров с интернет магазина Яндекса

    Args:
        page (str): Идентификатор последнего значения на странице.
                    Оставьте это поле пустым при выполнении первого запроса.
                    Чтобы получить следующие значения,
                    укажите nextPageToken из ответа предыдущего запроса.

        campaign_id (str): Идентификатор клиента.

        access_token (str): API-ключ

    Returns:
        словарь:
            "paging": (ScrollingPagerDTO) - Информация о страницах результатов.
                                            Ссылка на следующую страницу.
            "offerMappingEntries": 
                (OfferMappingEntryDTO[]) - Информация о товарах в каталоге.
                    name: (str) - Наименование
                    shopSku: (str) - Ваш SKU
                    category: (str) - Категория, к которой магазин относит свой товар

                    .... 
                    https://yandex.ru/dev/market/partner-api/doc/ru/reference/offer-mappings/getOfferMappingEntries#mappingsofferdto

    """



    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Передает данные об остатках товаров на витрине

    Args:
        stocks (list): "sku": (str) - Ваш SKU (Артикул) товара,
                       "warehouseId": (int) - Идентификатор склада,
                        "items": [
                            "count": (int) - Количество доступного товара,
                            "type": "FIT",
                            "updatedAt": (str) - Дата и время последнего 
                                                обновления информации об остатках 
                                                указанного типа.

                                            Формат даты и времени: ISO 8601 со 
                                            смещением относительно UTC. 
                                            Например, 2017-11-21T00:42:42+03:00.,

        campaign_id (str): Идентификатор клиента.

        access_token (str): API-ключ
    """

    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Установка цен

    Args:
        prices (list): "offerId": (str) - Ваш SKU (Артикул) товара,
                        "price": [
                            "value": (int) - Значение,
                            "currencyId": "RUR",

        campaign_id (str): Идентификатор клиента.

        access_token (str): API-ключ
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

    Args:
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        market_token (str): API-ключ

    Returns:
        Список артикулов
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """ Создает список склада

    Создается список артикла и колличеста из списка артикулов и остатков часв

    Если в остатках часов количество ">10" то заноситься количество 100
    Если остатки равны "1" или артикла нет в остатках заоситься количество 0

    Args:
        watch_remnants (list): список остатков часов
        offer_ids (list): Список артикулов
        warehouse_id ():

    Returns:
        Список из словарей:
            "sku": (str),Ваш SKU (Артикул) товара,
            "warehouseId": (int) - Идентификатор склада,
            "items": [
                "count": (int) - Количество доступного товара,
                "type": "FIT",
                "updatedAt": (str) - Дата и время обновления
    """

    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
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
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Обновляет цены товаров Яндекс маркета

    Получает артикулы товаров Яндекс маркета, 
    устанавливает цены и 
    обновляет цены товаров на  Яндекс маркете

    Args:
        watch_remnants (): Список часов casio
        campaign_id (str): Идентификатор кампании и идентификатор магазина
        market_token (str): API-ключ

    Returns:
        Список из словарей:
            "auto_action_enabled": "UNKNOWN"
            "currency_code": Валюта ("RUB")
            "offer_id": Артикул (из словаря watch_remnants)
            "old_price": Старая цена ("0")
            "price": Цена (из словаря watch_remnants)
    """

    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
