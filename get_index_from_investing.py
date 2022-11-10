import os
import asyncio
import contextlib
import re

from pyppeteer import launch
from dotenv import load_dotenv
import ujson
import redis

load_dotenv()


r = redis.Redis(
    host=os.environ.get('REDIS_URL', 'localhost'),
    port=os.environ.get('REDIS_PORT', 6379),
    password=os.environ.get('REDIS_PASSWORD', None),
    db=0,
    charset="utf-8",
    decode_responses=True
)

async def listen_websocket(url, indices_key):
    browser = await launch(
        headless=True,
        args=['--no-sandbox'],
        autoClose=True
    )
    page = await browser.newPage()
    await page.goto(url)

    extra_data = await page.querySelector('#__NEXT_DATA__')
    data_json_str = ujson.loads(await page.evaluate(
        '(id_information) => id_information.textContent',
        extra_data
    ))['props']['pageProps']['state']
    future_indices = ujson.loads(
        ujson.loads(data_json_str)['dataStore']['quotesStore']
    )['quotes'][indices_key]['_collection']
    id_to_indices = {
        future_index['instrumentId']: {
            'symbol': future_index['symbol'],
            'title': future_index['name']['title']
        }
        for future_index in future_indices
    }

    def print_message(response):
        with contextlib.suppress(Exception):
            messages = response['response']['payloadData'].replace(
                'a[', '[').replace('\\"', '"').replace(
                '}"', '}').replace('"{', '{').replace(
                '\\\\', '')
            messages = re.sub('"pid(.)*::{', '{', messages)
            messages = ujson.loads(messages)
            for message in messages:
                info = message['message']
                if int(info['pid']) in id_to_indices:
                    info.update(id_to_indices[int(info['pid'])])

                    # push to redis
                    symbol = info['symbol']
                    info_data = ujson.dumps(info)

                    if r.exists(symbol):
                        old_data = r.get(symbol)
                        if info_data != old_data:
                            r.publish(symbol, ujson.dumps(get_change_value(ujson.loads(r.get(symbol)) , info)))
                    else:
                        r.publish(symbol, info_data)

                    r.set(symbol, info_data)

    cdp = await page.target.createCDPSession()
    await cdp.send('Network.enable')
    await cdp.send('Page.enable')

    cdp.on('Network.webSocketFrameReceived', print_message)

def get_change_value(old_item, new_item):
    for key in new_item:
        if old_item[key] == new_item[key] and key not in ['pid', 'pc_col', 'symbol', 'title']:
            new_item[key] = None
    return new_item


async def main():
    await listen_websocket(
        url='https://www.investing.com/indices/indices-futures',
        indices_key='domain-indices_futures-indices.futures'
    )
    await listen_websocket(
        url='https://www.investing.com/indices/major-indices',
        indices_key='sml-74-indices.price'
    )
    while True:
        await asyncio.sleep(5)


asyncio.get_event_loop().run_until_complete(main())
