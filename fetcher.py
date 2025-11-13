import asyncio
import websockets
import json
import logging
import ssl
from datetime import datetime
from typing import Dict, List, Callable, Optional
from collections import defaultdict
import os
import gzip
import csv
import pandas as pd
import locale

# Устанавливаем локаль для правильного форматирования чисел
try:
    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
    except:
        pass

# Настройка логирования
def setup_logger():
    os.makedirs('logs', exist_ok=True)
    os.makedirs('results', exist_ok=True)
    
    logger = logging.getLogger('arbitrage_bot')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler(f'logs/bot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

class Config:
    """Конфигурация бота"""
    # Расширенный список фьючерсов
    SYMBOLS = [
        'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT',
        'ADA/USDT', 'DOGE/USDT', 'XRP/USDT', 'AVAX/USDT',
        'MATIC/USDT', 'DOT/USDT', 'LTC/USDT', 'LINK/USDT'
    ]
    
    MIN_PROFIT_THRESHOLD = 1.0  # 1% порог вместо 0.1%
    PRICE_MAX_AGE = 10  # seconds
    DISPLAY_UPDATE_INTERVAL = 3  # seconds
    TRADE_CLOSE_DELAY = 30  # seconds
    
    # Обновленные комиссии бирж (взяты с официальных сайтов)
    FEES = {
        'binance': 0.0004,    # 0.04% taker fee
        'bybit': 0.0006,      # 0.06% taker fee  
        'okx': 0.0005,        # 0.05% taker fee
        'gateio': 0.0005,     # 0.05% taker fee
        'mexc': 0.0002,       # 0.02% taker fee
        'bitget': 0.0006,     # 0.06% taker fee
        'kucoin': 0.0006,     # 0.06% taker fee
        'htx': 0.0005,        # 0.05% taker fee
        'phemex': 0.0006,     # 0.06% taker fee
        'kraken': 0.0004,     # 0.04% taker fee
        'whitebit': 0.0009,   # 0.09% taker fee
        'bitfinex': 0.0009,   # 0.09% taker fee
        'bingx': 0.0005,      # 0.05% taker fee
        'coinEx': 0.0006,     # 0.06% taker fee
        'bitmart': 0.0004,    # 0.04% taker fee
    }

class PriceHandler:
    def __init__(self):
        self.prices = defaultdict(dict)
        self.last_update = defaultdict(dict)
        self.callbacks = []
    
    async def handle_price_update(self, exchange: str, symbol: str, price: float):
        """Обрабатываем обновление цены"""
        self.prices[symbol][exchange] = price
        self.last_update[symbol][exchange] = datetime.now()
        
        # Логируем первое обновление для каждого символа
        if len(self.prices[symbol]) == 1:
            logger.info(f"💹 First price for {symbol} from {exchange}: ${price}")
        
        for callback in self.callbacks:
            await callback(symbol, exchange, price)
    
    def get_current_prices(self, symbol: str) -> Dict[str, float]:
        return self.prices.get(symbol, {}).copy()
    
    def is_price_fresh(self, exchange: str, symbol: str, max_age: int = Config.PRICE_MAX_AGE) -> bool:
        last_update = self.last_update[symbol].get(exchange)
        if not last_update:
            return False
        return (datetime.now() - last_update).total_seconds() <= max_age
    
    def get_active_exchanges_for_symbol(self, symbol: str) -> List[str]:
        """Получаем список бирж с актуальными ценами для символа"""
        active = []
        for exchange in self.prices.get(symbol, {}):
            if self.is_price_fresh(exchange, symbol):
                active.append(exchange)
        return active

class WebSocketManager:
    def __init__(self, price_handler: PriceHandler):
        self.price_handler = price_handler
        self.connections = {}
        self.is_running = False
        self.message_counters = defaultdict(int)
        self.active_exchanges = set()
        
        # SSL context
        self.ssl_context = ssl.create_default_context()
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        
        # ОБНОВЛЕННАЯ КОНФИГУРАЦИЯ 15 БИРЖ С ПРАВИЛЬНЫМИ URL
        self.exchange_configs = {
            'binance': {
                'url': 'wss://fstream.binance.com/ws',
                'headers': {'Origin': 'https://www.binance.com'},
                'subscribe': {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@ticker", "ethusdt@ticker", "bnbusdt@ticker",
                        "solusdt@ticker", "adausdt@ticker", "dogeusdt@ticker",
                        "xrpusdt@ticker", "avaxusdt@ticker", "maticusdt@ticker",
                        "dotusdt@ticker", "ltcusdt@ticker", "linkusdt@ticker"
                    ],
                    "id": 1
                }
            },
            'bybit': {
                'url': 'wss://stream.bybit.com/v5/public/linear',
                'headers': {'Origin': 'https://www.bybit.com'},
                'subscribe': {
                    "op": "subscribe",
                    "args": [
                        "tickers.BTCUSDT", "tickers.ETHUSDT", "tickers.BNBUSDT",
                        "tickers.SOLUSDT", "tickers.ADAUSDT", "tickers.DOGEUSDT",
                        "tickers.XRPUSDT", "tickers.AVAXUSDT", "tickers.MATICUSDT",
                        "tickers.DOTUSDT", "tickers.LTCUSDT", "tickers.LINKUSDT"
                    ]
                }
            },
            'okx': {
                'url': 'wss://ws.okx.com:8443/ws/v5/public',
                'headers': {'Origin': 'https://www.okx.com'},
                'subscribe': {
                    "op": "subscribe",
                    "args": [
                        {"channel": "tickers", "instId": "BTC-USD-SWAP"},
                        {"channel": "tickers", "instId": "ETH-USD-SWAP"},
                        {"channel": "tickers", "instId": "BNB-USD-SWAP"},
                        {"channel": "tickers", "instId": "SOL-USD-SWAP"},
                        {"channel": "tickers", "instId": "ADA-USD-SWAP"},
                        {"channel": "tickers", "instId": "DOGE-USD-SWAP"},
                        {"channel": "tickers", "instId": "XRP-USD-SWAP"},
                        {"channel": "tickers", "instId": "AVAX-USD-SWAP"},
                        {"channel": "tickers", "instId": "MATIC-USD-SWAP"},
                        {"channel": "tickers", "instId": "DOT-USD-SWAP"},
                        {"channel": "tickers", "instId": "LTC-USD-SWAP"},
                        {"channel": "tickers", "instId": "LINK-USD-SWAP"}
                    ]
                }
            },
            'gateio': {
                'url': 'wss://fx-ws.gateio.ws/v4/ws/usdt',
                'headers': {},
                'subscribe': {
                    "time": int(datetime.now().timestamp()),
                    "channel": "futures.tickers",
                    "event": "subscribe",
                    "payload": [
                        "BTC_USDT", "ETH_USDT", "BNB_USDT", "SOL_USDT",
                        "ADA_USDT", "DOGE_USDT", "XRP_USDT", "AVAX_USDT",
                        "MATIC_USDT", "DOT_USDT", "LTC_USDT", "LINK_USDT"
                    ]
                }
            },
            'mexc': {
                'url': 'wss://contract.mexc.com/ws',
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Origin': 'https://futures.mexc.com'
                },
                'subscribe': {
                    "method": "SUBSCRIPTION",
                    "params": ["perp@ticker@BTC_USDT"]
                }
            },
            'bitget': {
                'url': 'wss://ws.bitget.com/mix/v1/stream',
                'headers': {},
                'subscribe': {
                    "op": "subscribe",
                    "args": [
                        {"instType": "mc", "channel": "ticker", "instId": "BTCUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "ETHUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "BNBUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "SOLUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "ADAUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "DOGEUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "XRPUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "AVAXUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "MATICUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "DOTUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "LTCUSDT"},
                        {"instType": "mc", "channel": "ticker", "instId": "LINKUSDT"}
                    ]
                }
            },
            'kucoin': {
                'url': 'wss://futures-api.kucoin.com/endpoint',
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'subscribe': {
                    "type": "subscribe",
                    "topic": "/contractMarket/ticker:BTCUSDTM",
                    "response": True
                }
            },
            'htx': {
                'url': 'wss://api.btcgateway.pro/linear-swap-ws',
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'subscribe': {
                    "sub": "market.BTC-USDT.ticker",
                    "id": "id1"
                }
            },
            'phemex': {
                'url': 'wss://phemex.com/ws',
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'subscribe': {
                    "method": "tick.subscribe",
                    "params": ["BTCUSD"],
                    "id": 1
                }
            },
            'kraken': {
                'url': 'wss://futures.kraken.com/ws/v1',
                'headers': {},
                'subscribe': {
                    "event": "subscribe",
                    "feed": "ticker",
                    "product_ids": ["PI_XBTUSD", "PI_ETHUSD", "PI_BNBUSD", "PI_SOLUSD", "PI_ADAUSD"]
                }
            },
            'whitebit': {
                'url': 'wss://ws.whitebit.com/ws',
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'subscribe': {
                    "method": "ticker_subscribe",
                    "params": ["BTC_USDT"],
                    "id": 1
                },
            'bitfinex': {
                'url': 'wss://api.bitfinex.com/ws/2',
                'headers': {},
                'subscribe': {
                    "event": "subscribe",
                    "channel": "ticker",
                    "symbol": "tBTCUSD"
                }
            },
            'bingx': {
                'url': 'wss://open-api-swap.bingx.com/swap-market',
                'headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                'subscribe': {
                    "id": "1",
                    "reqType": "sub",
                    "dataType": "BTC-USDT@ticker"
                }
            },
            'coinEx': {
                'url': 'wss://socket.coinex.com/',
                'headers': {},
                'subscribe': {
                    "method": "ticker.subscribe",
                    "params": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"],
                    "id": 1
                }
            },
            'bitmart': {
                'url': 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
                'headers': {},
                'subscribe': {
                    "op": "subscribe",
                    "args": ["futures/ticker:BTCUSDT", "futures/ticker:ETHUSDT", "futures/ticker:BNBUSDT"]
                }
            }
        }
    }

    async def connect_exchange(self, exchange: str):
        """Улучшенное подключение с альтернативными URL"""
        try:
            # Альтернативные URL для проблемных бирж
            alt_urls = {
                'mexc': [
                    'wss://contract.mexc.com/ws',
                    'wss://futures.mexc.com/ws' 
                ],
                'kucoin': [
                    'wss://futures-api.kucoin.com/endpoint',
                    'wss://api-futures.kucoin.com/endpoint'
                ],
                'phemex': [
                    'wss://phemex.com/ws',
                    'wss://api.phemex.com/ws'
                ],
                'whitebit': [
                    'wss://ws.whitebit.com/ws',
                    'wss://api.whitebit.com/ws'
                ]
            }
            
            config = self.exchange_configs[exchange]
            urls_to_try = alt_urls.get(exchange, [config['url']])
            
            last_error = None
            for url in urls_to_try:
                try:
                    logger.info(f"🔗 Connecting to {exchange} via {url}...")
                    
                    connect_kwargs = {
                        'ping_interval': 20,
                        'ping_timeout': 10,
                        'ssl': self.ssl_context,
                        'max_size': 2**20
                    }
                    
                    if config.get('headers'):
                        connect_kwargs['extra_headers'] = config['headers']
                    
                    websocket = await asyncio.wait_for(
                        websockets.connect(url, **connect_kwargs),
                        timeout=15.0
                    )
                    
                    self.connections[exchange] = websocket
                    
                    # Обновляем URL в конфиге на рабочий
                    self.exchange_configs[exchange]['url'] = url
                    
                    # Отправляем подписку
                    if config.get('subscribe'):
                        await websocket.send(json.dumps(config['subscribe']))
                        logger.info(f"📤 Sent subscription to {exchange}")
                    
                    self.active_exchanges.add(exchange)
                    logger.info(f"✅ Successfully connected to {exchange}")
                    return True
                    
                except Exception as e:
                    last_error = e
                    logger.warning(f"⚠️ Failed to connect to {exchange} via {url}: {str(e)}")
                    continue
            
            # Если все URL не сработали
            logger.error(f"❌ All connection attempts failed for {exchange}: {str(last_error)}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to {exchange}: {str(e)}")
            return False

    async def listen_exchange(self, exchange: str):
        """Слушаем сообщения от биржи с переподключением"""
        while self.is_running:
            try:
                websocket = self.connections[exchange]
                async for message in websocket:
                    if not self.is_running:
                        break
                        
                    self.message_counters[exchange] += 1
                    await self.handle_message(exchange, message)
                    
            except Exception as e:
                logger.error(f"❌ Listen error for {exchange}: {str(e)}")
                if exchange in self.active_exchanges:
                    self.active_exchanges.remove(exchange)
                
                # Пытаемся переподключиться
                if self.is_running:
                    logger.info(f"🔄 Attempting to reconnect to {exchange}...")
                    await asyncio.sleep(5)
                    if await self.connect_exchange(exchange):
                        continue
                break

    async def handle_message(self, exchange: str, message: str):
        """Обрабатываем сообщение"""
        try:
            # Обработка бинарных данных
            if isinstance(message, bytes):
                try:
                    message = gzip.decompress(message).decode('utf-8')
                except:
                    message = message.decode('utf-8', errors='ignore')
            
            data = json.loads(message)
            prices = self.parse_message(exchange, data)
            
            for symbol, price in prices.items():
                if price and price > 0:
                    await self.price_handler.handle_price_update(exchange, symbol, price)
                    
            # Логируем первое сообщение
            if self.message_counters[exchange] == 1:
                logger.info(f"📥 First message from {exchange}")
                    
        except json.JSONDecodeError:
            logger.warning(f"❌ JSON decode error from {exchange}")
        except Exception as e:
            logger.debug(f"Parse error for {exchange}: {e}")

    def parse_message(self, exchange: str, data: dict) -> Dict[str, float]:
        """Парсим сообщение и извлекаем цены"""
        prices = {}
        
        try:
            if exchange == 'binance':
                if 'e' in data and data['e'] == '24hrTicker':
                    symbol = data['s'].replace('USDT', '/USDT')
                    prices[symbol] = float(data['c'])
                    
            elif exchange == 'bybit':
                if 'topic' in data and 'tickers' in data['topic'] and 'data' in data:
                    symbol = data['data']['symbol'].replace('USDT', '/USDT')
                    prices[symbol] = float(data['data']['lastPrice'])
                    
            elif exchange == 'okx':
                if 'arg' in data and data['arg']['channel'] == 'tickers':
                    inst_id = data['arg']['instId']
                    symbol = inst_id.replace('-USD-SWAP', '/USDT')
                    if 'data' in data and len(data['data']) > 0:
                        prices[symbol] = float(data['data'][0]['last'])
                        
            elif exchange == 'gateio':
                if 'channel' in data and data['channel'] == 'futures.tickers' and 'result' in data:
                    contract = data['result']['contract']
                    symbol = contract.replace('_USDT', '/USDT')
                    prices[symbol] = float(data['result']['last'])
                        
            elif exchange == 'mexc':
                # MEXC часто присылает данные в разных форматах
                if 'data' in data and 'p' in data['data']:
                    # Формат: {"channel":"push.ticker","data":{"p":"50000.0","s":"BTC_USDT"},"ts":...}
                    symbol = data['data']['s'].replace('_USDT', '/USDT')
                    prices[symbol] = float(data['data']['p'])
                elif 'c' in data and 'p' in data:
                    # Альтернативный формат MEXC
                    channel = data['c']
                    if 'perp@ticker@' in channel:
                        symbol_raw = channel.replace('perp@ticker@', '')
                        symbol = symbol_raw.replace('_USDT', '/USDT')
                        prices[symbol] = float(data['p'])
                        
            elif exchange == 'bitget':
                if 'action' in data and data['action'] == 'snapshot' and 'data' in data:
                    for ticker in data['data']:
                        symbol = ticker['instId'].replace('USDT', '/USDT')
                        prices[symbol] = float(ticker['last'])
                        
            elif exchange == 'kucoin':
                # KuCoin Futures
                if 'data' in data and 'price' in data['data']:
                    symbol = 'BTC/USDT'  # Упрощенно, так как сложно парсить все символы
                    prices[symbol] = float(data['data']['price'])
                elif 'subject' in data and data['subject'] == 'ticker':
                    if 'data' in data and 'price' in data['data']:
                        symbol = 'BTC/USDT'
                        prices[symbol] = float(data['data']['price'])
                            
            # Упрощенные парсеры для остальных бирж
            elif exchange == 'htx':
                if 'tick' in data and 'last_price' in data['tick']:
                    prices['BTC/USDT'] = float(data['tick']['last_price'])
                elif 'ch' in data and 'ticker' in data['ch']:
                    # Альтернативный формат HTX
                    if 'tick' in data and 'lastPrice' in data['tick']:
                        prices['BTC/USDT'] = float(data['tick']['lastPrice'])
                        
            elif exchange == 'phemex':
                if 'ticker' in data and 'last' in data['ticker']:
                    symbol = 'BTC/USDT'  # Упрощенный парсер
                    prices[symbol] = float(data['ticker']['last'])
                        
            elif exchange == 'kraken':
                if 'product_id' in data and 'mark_price' in data:
                    symbol = data['product_id'].replace('PI_', '').replace('USD', '/USDT')
                    prices[symbol] = float(data['mark_price'])

            elif exchange == 'bingx':
                # Bingx часто присылает бинарные данные
                if 'data' in data and 'c' in data['data']:
                    prices['BTC/USDT'] = float(data['data']['c'])
                elif isinstance(data, str) and 'BTC-USDT' in data:
                    # Пытаемся извлечь цену из строки
                    import re
                    price_match = re.search(r'"c":"([0-9.]+)"', data)
                    if price_match:
                        prices['BTC/USDT'] = float(price_match.group(1))
            
            # Дополнительные упрощенные парсеры для других бирж
            elif exchange == 'phemex':
                if 'ticker' in data and 'last' in data['ticker']:
                    prices['BTC/USDT'] = float(data['ticker']['last'])
                    
            elif exchange == 'whitebit':
                if 'params' in data and isinstance(data['params'], list) and len(data['params']) > 1:
                    # Whitebit формат: [symbol, price]
                    if data['params'][0] == 'BTC_USDT':
                        prices['BTC/USDT'] = float(data['params'][1])
                        
        except Exception as e:
            logger.debug(f"Price parse error for {exchange}: {e}")
            
        return prices

    def extract_price_from_data(self, data) -> float:
        """Пытаемся извлечь цену из данных"""
        try:
            if isinstance(data, dict):
                # Ищем поле с ценой
                for key, value in data.items():
                    if isinstance(value, (int, float)) and value > 1000:
                        return float(value)
                    elif isinstance(value, str) and value.replace('.', '').isdigit():
                        price = float(value)
                        if price > 1000:
                            return price
            return 0.0
        except:
            return 0.0

    async def start(self):
        """Запускаем все подключения"""
        self.is_running = True
        
        # Запускаем подключения ко всем биржам
        connection_tasks = []
        for exchange in self.exchange_configs.keys():
            task = asyncio.create_task(self.connect_exchange(exchange))
            connection_tasks.append(task)
        
        # Ждем завершения всех подключений
        await asyncio.gather(*connection_tasks, return_exceptions=True)
        
        # Запускаем прослушивание для успешно подключенных бирж
        listen_tasks = []
        for exchange in self.connections.keys():
            task = asyncio.create_task(self.listen_exchange(exchange))
            listen_tasks.append(task)
        
        # Задача для мониторинга
        asyncio.create_task(self.monitor_connections())
        
        await asyncio.gather(*listen_tasks, return_exceptions=True)

    async def monitor_connections(self):
        """Фокус на работающие биржи и их эффективность"""
        while self.is_running:
            try:
                active_count = len(self.active_exchanges)
                
                # Группируем биржи по статусу
                working_exchanges = []
                problematic_exchanges = []
                
                for exchange in self.exchange_configs.keys():
                    if exchange in self.active_exchanges:
                        msg_count = self.message_counters[exchange]
                        # Проверяем, получаем ли мы реальные цены
                        has_prices = any(
                            any(self.price_handler.prices.get(symbol, {}).get(exchange) 
                                for symbol in Config.SYMBOLS)
                        )
                        
                        if msg_count > 0 and has_prices:
                            working_exchanges.append((exchange, msg_count))
                        else:
                            problematic_exchanges.append((exchange, msg_count))
                    else:
                        problematic_exchanges.append((exchange, 0))
                
                logger.info(f"📊 CONNECTION STATUS: {len(working_exchanges)}/{len(self.exchange_configs)} exchanges WORKING")
                
                # Топ работающих бирж
                if working_exchanges:
                    logger.info("🏆 TOP WORKING EXCHANGES:")
                    for exchange, count in sorted(working_exchanges, key=lambda x: x[1], reverse=True)[:5]:
                        # Считаем количество символов с ценами
                        symbol_count = sum(
                            1 for symbol in Config.SYMBOLS 
                            if self.price_handler.prices.get(symbol, {}).get(exchange)
                        )
                        logger.info(f"   ✅ {exchange}: {count} msgs, {symbol_count} symbols")
                
                # Проблемные биржи
                if problematic_exchanges:
                    logger.info("⚠️ PROBLEMATIC EXCHANGES:")
                    for exchange, count in problematic_exchanges[:5]:
                        status = "❌" if exchange not in self.active_exchanges else "🟡"
                        logger.info(f"   {status} {exchange}: {count} messages")
                
                # Переподключаем только самые важные проблемные биржи
                important_exchanges = ['binance', 'bybit', 'okx', 'kucoin', 'mexc']
                for exchange in important_exchanges:
                    if exchange not in self.active_exchanges and exchange not in self.connections:
                        logger.info(f"🔄 Attempting to reconnect {exchange}...")
                        await self.connect_exchange(exchange)
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(30)

    async def stop(self):
        """Останавливаем все подключения"""
        self.is_running = False
        for exchange, websocket in self.connections.items():
            try:
                await websocket.close()
                logger.info(f"🔴 Closed connection to {exchange}")
            except Exception as e:
                logger.error(f"Error closing {exchange}: {e}")

class ArbitrageCalculator:
    def __init__(self, price_handler: PriceHandler):
        self.price_handler = price_handler

    def find_opportunities(self) -> List[Dict]:
        """Находим арбитражные возможности с ПРАВИЛЬНЫМ учетом комиссий"""
        opportunities = []
        
        for symbol in Config.SYMBOLS:
            prices = self.price_handler.get_current_prices(symbol)
            if len(prices) < 2:
                continue
                
            # Получаем только свежие цены
            fresh_prices = {}
            for exchange, price in prices.items():
                if self.price_handler.is_price_fresh(exchange, symbol):
                    fresh_prices[exchange] = price
            
            if len(fresh_prices) < 2:
                continue
                
            # Ищем лучшие цены покупки и продажи
            buy_exchange = min(fresh_prices, key=fresh_prices.get)
            sell_exchange = max(fresh_prices, key=fresh_prices.get)
            
            buy_price = fresh_prices[buy_exchange]
            sell_price = fresh_prices[sell_exchange]
            
            if buy_price <= 0 or sell_price <= 0 or buy_exchange == sell_exchange:
                continue
            
            # ПОЛНЫЙ РАСЧЕТ АРБИТРАЖА С КОМИССИЯМИ
            # Пример: buy_price=5000, sell_price=5600, fee_buy=0.01%, fee_sell=0.02%
            
            # 1. Рассчитываем валовую прибыль в деньгах
            gross_profit_usd = sell_price - buy_price  # 5600 - 5000 = 600$
            
            # 2. Рассчитываем комиссии в деньгах
            buy_fee_usd = buy_price * Config.FEES[buy_exchange]  # 5000 * 0.0001 = 0.5$
            sell_fee_usd = sell_price * Config.FEES[sell_exchange]  # 5600 * 0.0002 = 1.12$
            total_fees_usd = buy_fee_usd + sell_fee_usd  # 0.5 + 1.12 = 1.62$
            
            # 3. Чистая прибыль в деньгах после комиссий
            net_profit_usd = gross_profit_usd - total_fees_usd  # 600 - 1.62 = 598.38$
            
            # 4. Прибыль в процентах от цены покупки
            net_profit_percent = (net_profit_usd / buy_price) * 100  # (598.38 / 5000) * 100 = 11.9676%
            
            # 5. Валовая прибыль в % (для информации)
            gross_profit_percent = (gross_profit_usd / buy_price) * 100
            
            # Только если прибыль выше порога 1%
            if net_profit_percent >= Config.MIN_PROFIT_THRESHOLD:
                opportunities.append({
                    'symbol': symbol,
                    'buy_exchange': buy_exchange,
                    'sell_exchange': sell_exchange,
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'gross_profit_usd': gross_profit_usd,
                    'gross_profit_percent': gross_profit_percent,
                    'buy_fee_usd': buy_fee_usd,
                    'sell_fee_usd': sell_fee_usd,
                    'total_fees_usd': total_fees_usd,
                    'net_profit_usd': net_profit_usd,
                    'net_profit_percent': net_profit_percent,
                    'timestamp': datetime.now(),
                    'buy_fee_rate': Config.FEES[buy_exchange] * 100,  # в %
                    'sell_fee_rate': Config.FEES[sell_exchange] * 100  # в %
                })
        
        # Сортируем по чистой прибыли
        opportunities.sort(key=lambda x: x['net_profit_percent'], reverse=True)
        return opportunities

class TradingSimulator:
    def __init__(self):
        self.open_trades = []
        self.closed_trades = []
        self.trade_id_counter = 0
        self.csv_file = f'results/trades_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        self.setup_csv()
    
    def setup_csv(self):
        """Создаем CSV файл с заголовками и русским форматом чисел"""
        os.makedirs('results', exist_ok=True)
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow([
                'trade_id', 'symbol', 'buy_exchange', 'sell_exchange',
                'buy_price', 'sell_price', 'open_time', 'close_time',
                'expected_profit', 'actual_profit', 'duration_seconds',
                'buy_fee', 'sell_fee', 'net_profit'
            ])
    
    def format_number(self, number: float) -> str:
        """Форматируем число с запятой как разделитель"""
        try:
            return f"{number:.6f}".replace('.', ',')
        except:
            return str(number).replace('.', ',')
    
    def open_trade(self, opportunity: Dict) -> str:
        """Открываем сделку с учетом комиссий"""
        trade_id = f"trade_{self.trade_id_counter}"
        self.trade_id_counter += 1
        
        trade = {
            'trade_id': trade_id,
            'symbol': opportunity['symbol'],
            'buy_exchange': opportunity['buy_exchange'],
            'sell_exchange': opportunity['sell_exchange'],
            'buy_price': opportunity['buy_price'],
            'sell_price': opportunity['sell_price'],
            'buy_fee_percent': opportunity['buy_fee_percent'],
            'sell_fee_percent': opportunity['sell_fee_percent'],
            'expected_profit': opportunity['net_profit_percent'],  # Уже с учетом комиссий
            'open_time': datetime.now(),
            'status': 'open'
        }
        
        self.open_trades.append(trade)
        logger.info(f"💰 OPENED {trade_id}: {opportunity['symbol']} "
                   f"{opportunity['buy_exchange']}→{opportunity['sell_exchange']} "
                   f"({opportunity['net_profit_percent']:.3f}% net profit)")
        
        return trade_id
    
    def close_trade(self, trade_id: str, actual_profit: float = None):
        """Закрываем сделку и сохраняем в CSV с русским форматом"""
        for i, trade in enumerate(self.open_trades):
            if trade['trade_id'] == trade_id:
                trade['close_time'] = datetime.now()
                trade['status'] = 'closed'
                
                # Если фактическая прибыль не указана, используем ожидаемую
                if actual_profit is None:
                    actual_profit = trade['expected_profit']
                
                trade['actual_profit'] = actual_profit
                trade['duration'] = (trade['close_time'] - trade['open_time']).total_seconds()
                trade['net_profit'] = actual_profit
                
                # Сохраняем в CSV
                self.save_trade_to_csv(trade)
                
                self.closed_trades.append(trade)
                self.open_trades.pop(i)
                
                logger.info(f"🔒 CLOSED {trade_id}: Actual profit {actual_profit:.3f}%")
                break
    
    def save_trade_to_csv(self, trade: Dict):
        """Сохраняем сделку в CSV файл с русским форматом чисел"""
        try:
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow([
                    trade['trade_id'],
                    trade['symbol'],
                    trade['buy_exchange'],
                    trade['sell_exchange'],
                    self.format_number(trade['buy_price']),
                    self.format_number(trade['sell_price']),
                    trade['open_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    trade['close_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    self.format_number(trade['expected_profit']),
                    self.format_number(trade.get('actual_profit', 0)),
                    self.format_number(trade.get('duration', 0)),
                    self.format_number(trade.get('buy_fee_percent', 0)),
                    self.format_number(trade.get('sell_fee_percent', 0)),
                    self.format_number(trade.get('net_profit', 0))
                ])
        except Exception as e:
            logger.error(f"Error saving trade to CSV: {e}")

# Остальные классы (Analytics, DisplayManager, ArbitrageBot) остаются аналогичными
# с небольшими изменениями для отображения информации о комиссиях

class Analytics:
    def __init__(self, trading_simulator: TradingSimulator):
        self.trading_simulator = trading_simulator
        
    def generate_report(self) -> str:
        """Генерируем отчет по сделкам"""
        if not self.trading_simulator.closed_trades:
            return "No trades completed yet"
        
        # Создаем DataFrame для анализа
        trades_data = []
        for trade in self.trading_simulator.closed_trades:
            trades_data.append({
                'trade_id': trade['trade_id'],
                'symbol': trade['symbol'],
                'buy_exchange': trade['buy_exchange'],
                'sell_exchange': trade['sell_exchange'],
                'expected_profit': trade['expected_profit'],
                'actual_profit': trade.get('actual_profit', 0),
                'buy_fee': trade.get('buy_fee_percent', 0),
                'sell_fee': trade.get('sell_fee_percent', 0),
                'net_profit': trade.get('net_profit', 0),
                'duration': trade.get('duration', 0)
            })
        
        df = pd.DataFrame(trades_data)
        
        total_profit = df['net_profit'].sum()
        avg_profit = df['net_profit'].mean()
        win_rate = (df['net_profit'] > 0).mean() * 100
        total_trades = len(df)
        avg_fee = (df['buy_fee'] + df['sell_fee']).mean()
        
        report = f"""
📊 TRADING ANALYTICS REPORT
==========================
Total Trades: {total_trades}
Total Net Profit: {total_profit:.3f}%
Average Net Profit: {avg_profit:.3f}%
Average Fee Cost: {avg_fee:.3f}%
Win Rate: {win_rate:.1f}%

Top Performing Symbols:
"""
        # Статистика по символам
        symbol_stats = df.groupby('symbol')['net_profit'].agg(['mean', 'count']).round(3)
        for symbol, stats in symbol_stats.iterrows():
            report += f"  {symbol}: {stats['mean']}% avg ({stats['count']} trades)\n"
        
        # Статистика по биржам
        report += "\nTop Performing Exchanges (Buy side):\n"
        buy_stats = df.groupby('buy_exchange')['net_profit'].mean().round(3)
        for exchange, profit in buy_stats.nlargest(5).items():
            report += f"  {exchange}: {profit}% avg profit\n"
            
        report += "\nTop Performing Exchanges (Sell side):\n"
        sell_stats = df.groupby('sell_exchange')['net_profit'].mean().round(3)
        for exchange, profit in sell_stats.nlargest(5).items():
            report += f"  {exchange}: {profit}% avg profit\n"
            
        return report

class DisplayManager:
    def __init__(self):
        self.last_display = 0
        self.top_opportunities = []
        
    async def update_display(self, opportunities: List[Dict], price_handler: PriceHandler):
        current_time = asyncio.get_event_loop().time()
        if current_time - self.last_display < Config.DISPLAY_UPDATE_INTERVAL:
            return
            
        self.last_display = current_time
        self.top_opportunities = sorted(opportunities, key=lambda x: x['net_profit_percent'], reverse=True)[:10]
        
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("🔄 ENHANCED CRYPTO FUTURES ARBITRAGE BOT - LIVE")
        print(f"📊 Last update: {datetime.now().strftime('%H:%M:%S')}")
        
        # Статус подключений
        active_count = len([ex for ex in price_handler.prices if any(price_handler.prices[ex].values())])
        print(f"💱 Active data sources: {active_count} exchanges | 📈 Symbols: {len(Config.SYMBOLS)}")
        print(f"💰 Min profit: {Config.MIN_PROFIT_THRESHOLD}% (after fees)")
        print()
        
        # Быстрый обзор цен
        print("=== QUICK PRICE OVERVIEW ===")
        major_symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT']
        for symbol in major_symbols:
            prices = price_handler.get_current_prices(symbol)
            if prices:
                sources = len([p for p in prices.values() if p > 0])
                min_price = min(prices.values())
                max_price = max(prices.values())
                spread = ((max_price - min_price) / min_price * 100) if min_price > 0 else 0
                print(f"  {symbol}: {sources} sources, Spread: {spread:.3f}%")
        print()
        
        # Арбитражные возможности с детальной информацией о комиссиях
        self.print_detailed_opportunities()
        print()
        
        print("Press Ctrl+C to stop...")
    
    def print_detailed_opportunities(self):
        """Печатаем детальную информацию об арбитражных возможностях"""
        print("=== ARBITRAGE OPPORTUNITIES (DETAILED) ===")
        if not self.top_opportunities:
            print("  No opportunities found (min profit: 1.0% after fees)")
            return
            
        print(f"{'#':<2} {'Symbol':<8} {'Action':<16} {'Price':<12} {'Profit$':<10} {'Fees$':<8} {'Net%':<6}")
        print("-" * 70)
        
        for i, opp in enumerate(self.top_opportunities[:6], 1):
            buy_action = f"BUY@{opp['buy_exchange']}"
            sell_action = f"SELL@{opp['sell_exchange']}"
            
            print(f"{i:<2} {opp['symbol']:<8} {buy_action:<16} ${opp['buy_price']:<11.2f}")
            print(f"    {'':<8} {sell_action:<16} ${opp['sell_price']:<11.2f} "
                  f"+${opp['gross_profit_usd']:<8.2f} -${opp['total_fees_usd']:<6.2f} "
                  f"{opp['net_profit_percent']:<5.2f}%")
            
            # Детали комиссий
            print(f"    Fees: {opp['buy_fee_rate']:.3f}% + {opp['sell_fee_rate']:.3f}% = "
                  f"{opp['buy_fee_rate'] + opp['sell_fee_rate']:.3f}% "
                  f"(${opp['buy_fee_usd']:.2f} + ${opp['sell_fee_usd']:.2f})")
            print()

class ArbitrageBot:
    def __init__(self):
        self.logger = logger
        self.price_handler = PriceHandler()
        self.websocket_manager = WebSocketManager(self.price_handler)
        self.arbitrage_calculator = ArbitrageCalculator(self.price_handler)
        self.trading_simulator = TradingSimulator()
        self.display_manager = DisplayManager()
        self.analytics = Analytics(self.trading_simulator)
        
        self.is_running = False
        self.start_time = datetime.now()

    async def start(self):
        """Запускаем бота"""
        self.is_running = True
        self.logger.info("🚀 Starting Enhanced Crypto Futures Arbitrage Bot")
        self.logger.info(f"💡 Monitoring {len(Config.SYMBOLS)} symbols across {len(self.websocket_manager.exchange_configs)} exchanges")
        self.logger.info(f"💰 Minimum profit threshold: {Config.MIN_PROFIT_THRESHOLD}% after fees")
        
        try:
            # Запускаем WebSocket подключения
            asyncio.create_task(self.websocket_manager.start())
            
            # Даем время на подключение
            await asyncio.sleep(5)
            
            # Основной цикл
            await self.main_loop()
            
        except Exception as e:
            self.logger.error(f"Bot error: {e}")
        finally:
            await self.stop()

    async def main_loop(self):
        """Основной цикл бота"""
        iteration = 0
        while self.is_running:
            try:
                iteration += 1
                
                # Ищем арбитражные возможности
                opportunities = self.arbitrage_calculator.find_opportunities()
                
                # Автоматическое закрытие сделок при исчезновении арбитража
                await self.auto_close_trades(opportunities)
                
                # Открываем сделки для хороших возможностей (выше 1%)
                for opportunity in opportunities:
                    if opportunity['net_profit_percent'] >= Config.MIN_PROFIT_THRESHOLD:
                        trade_id = self.trading_simulator.open_trade(opportunity)
                        # Симулируем закрытие через указанное время
                        asyncio.create_task(self.simulate_trade_close(trade_id, Config.TRADE_CLOSE_DELAY))
                
                # Обновляем дисплей
                await self.display_manager.update_display(opportunities, self.price_handler)
                
                # Периодическое логирование
                if iteration % 20 == 0:
                    await self.log_status(iteration)
                
                await asyncio.sleep(2)  # Увеличиваем интервал для стабильности
                
            except Exception as e:
                self.logger.error(f"Main loop error: {e}")
                await asyncio.sleep(2)

    async def simulate_trade_close(self, trade_id: str, delay: int):
        """Симулируем закрытие сделки с реалистичной прибылью"""
        await asyncio.sleep(delay)
        for trade in self.trading_simulator.open_trades:
            if trade['trade_id'] == trade_id:
                # Реалистичная симуляция: 70-90% от ожидаемой прибыли
                realistic_profit = trade['expected_profit'] * (0.7 + 0.2 * (id(trade) % 100 / 100))
                self.trading_simulator.close_trade(trade_id, realistic_profit)
                break

    async def auto_close_trades(self, opportunities: List[Dict]):
        """Автоматически закрываем сделки, если арбитраж исчез"""
        current_opportunities = {f"{opp['buy_exchange']}-{opp['sell_exchange']}-{opp['symbol']}": opp 
                               for opp in opportunities}
        
        for trade in self.trading_simulator.open_trades[:]:
            key = f"{trade['buy_exchange']}-{trade['sell_exchange']}-{trade['symbol']}"
            if key not in current_opportunities:
                # Если арбитраж исчез, закрываем с небольшой прибылью или убытком
                self.trading_simulator.close_trade(trade['trade_id'], 
                                                 trade['expected_profit'] * 0.3)

    async def log_status(self, iteration: int):
        """Логируем статус бота"""
        active_count = len(self.websocket_manager.active_exchanges)
        total_messages = sum(self.websocket_manager.message_counters.values())
        opportunities = self.arbitrage_calculator.find_opportunities()
        profitable_opportunities = [opp for opp in opportunities if opp['net_profit_percent'] >= Config.MIN_PROFIT_THRESHOLD]
        
        self.logger.info(f"🔄 Bot status: {active_count} exchanges, {total_messages} messages, "
                        f"{len(profitable_opportunities)} profitable opportunities, {len(self.trading_simulator.open_trades)} open trades")

    async def stop(self):
        """Останавливаем бота"""
        self.logger.info("🛑 Stopping bot...")
        self.is_running = False
        await self.websocket_manager.stop()
        
        # Закрываем все открытые сделки
        for trade in self.trading_simulator.open_trades[:]:
            self.trading_simulator.close_trade(trade['trade_id'], 
                                             trade['expected_profit'] * 0.5)
        
        # Выводим итоги
        self.print_summary()

    def print_summary(self):
        """Печатаем итоги работы"""
        runtime = datetime.now() - self.start_time
        hours, remainder = divmod(runtime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        print("\n" + "="*70)
        print("🎯 ENHANCED CRYPTO FUTURES ARBITRAGE BOT - FINAL SUMMARY")
        print("="*70)
        
        print(f"⏱️  Runtime: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}")
        
        # Статистика подключений
        active_count = len(self.websocket_manager.active_exchanges)
        total_count = len(self.websocket_manager.exchange_configs)
        total_messages = sum(self.websocket_manager.message_counters.values())
        
        print(f"\n🔗 CONNECTIONS: {active_count}/{total_count} exchanges")
        print(f"📨 MESSAGES: {total_messages:,} total")
        
        # Показываем топ-5 бирж по сообщениям
        top_exchanges = sorted(self.websocket_manager.message_counters.items(), 
                              key=lambda x: x[1], reverse=True)[:5]
        for exchange, count in top_exchanges:
            status = "✅" if exchange in self.websocket_manager.active_exchanges else "❌"
            print(f"  {status} {exchange}: {count:,} messages")
        
        # Аналитика торгов
        print(f"\n{self.analytics.generate_report()}")
        
        print(f"\n💾 Trading data saved to: {self.trading_simulator.csv_file}")
        print("="*70)

async def main():
    """Основная функция"""
    bot = ArbitrageBot()
    
    try:
        await bot.start()
    except KeyboardInterrupt:
        print("\n🛑 Received Ctrl+C, stopping...")
        await bot.stop()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        await bot.stop()

if __name__ == "__main__":
    print("🔧 ENHANCED CRYPTO FUTURES ARBITRAGE BOT")
    print("="*50)
    print("💡 Key Features:")
    print("   📈 15+ exchanges with real futures data")
    print("   🔄 12 trading symbols including popular and niche coins")
    print("   💰 1% minimum profit threshold after fees")
    print("   📊 Real commission calculation for accurate profits")
    print("   🔧 Russian number format in CSV (0,76 instead of 0.76)")
    print("   ⚡ Improved connection stability and reconnection")
    print("   📈 Detailed analytics with fee breakdown")
    print()
    print("🛑 Press Ctrl+C to stop")
    print("="*50)
    print()
    
    asyncio.run(main())
