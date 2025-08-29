"""
This bot searches opportunities for time arbitrage on illiquid or emerging pairs. It exploits price 
discrepancies arising due to low volumes of trade. It works better during off-peak hours with the
lowest liquidity.
"""

import json
import asyncio
import json
import re
import sys
import queue
import time
import threading
import ccxt.pro as ccxtpro
from typing import Callable
from pprint import pprint
from bot1 import \
    fetch_price, fetch_all_prices_and_fees, calculate_volatility, \
    calculate_fiat_amount_to_trade, find_best_difference, get_all_available_trade_pairs, round_3_sig_fig


def exponential_backoff(async_function: Callable) -> Callable:
    async def wrapper(*args, **kwargs):
        for attempt in range(7):
            try:
                return await async_function(*args, **kwargs)
            except:
                print(f"Retrying to perform {async_function}. Attempt {attempt}")
                await asyncio.sleep(2 ** attempt)
        raise Exception("During exponential backoff, an error occurred.")
    return wrapper

async def arbitrage(operate: Callable[[str], None]) -> None:    
    global trade_pairs_in_operation
    time_now = time.time()
    for trade_pair in trade_pairs_in_operation:
        if trade_pair in time_of_last_trade:
            if time_now - time_of_last_trade[trade_pair] > TIME_THRESHOLD:
                print(f"Removing {trade_pair} from operation")
                trade_pairs_in_operation.remove(trade_pair)
                await close_positions(trade_pair, EXCHANGES)
    if len(trade_pairs_in_operation) == TRADE_PAIRS_LIMIT:
        illiquid_pairs = await catch_illiquid_pairs_in_operation()
    else:
        illiquid_pairs = await search_for_illiquid_pairs()
        if illiquid_pairs == []:
            await asyncio.sleep(30)
    print(illiquid_pairs)
    tasks = [operate(trade_pair) for trade_pair in illiquid_pairs]
    await asyncio.gather(*tasks)

async def operate(trade_pair: str) -> None:
    global trade_pairs_in_operation
    global start_mean_prices
    buy_exchange, sell_exchange, transaction_difference, nominal_price = await find_opportunity(trade_pair)
    if buy_exchange is sell_exchange:
        return None
    volatility = calculate_volatility(trade_pair, significance_level=0.10, time=5)  # Higher time for more stability
    amount = await calculate_half_the_lowest_amount(buy_exchange, sell_exchange, trade_pair.split('/')[0])

    # If there is space left to operate an additional trade_pair - open parallel positions
    if trade_pair not in trade_pairs_in_operation and len(trade_pairs_in_operation) < TRADE_PAIRS_LIMIT:
        trade_pairs_in_operation.append(trade_pair)
        exchanges = [buy_exchange, sell_exchange]
        open_amount = calculate_fiat_amount_to_trade(open_amount_USDT, trade_pair, nominal_price)
        await open_parallel_positions(trade_pair, exchanges, open_amount)
        if len(trade_pairs_in_operation) == TRADE_PAIRS_LIMIT:
            start_mean_prices = await register_mean_prices()
        return None

    # Checking whether a pair is not in already operated pairs and the limit is surpassed
    if trade_pair not in trade_pairs_in_operation and len(trade_pairs_in_operation) >= TRADE_PAIRS_LIMIT:
        return None

    if amount * nominal_price < 1:
        _ = await execute_rebalancing(buy_exchange, sell_exchange, trade_pair)
        return None

    # By this point the trade_pair must be in trade_pairs_in_operation
    if transaction_difference > volatility and len(trade_pairs_in_operation) == TRADE_PAIRS_LIMIT:
        print(f"Difference - {transaction_difference}, amount - {amount} {trade_pair.split('/')[0]}")
        await place_limit_orders_with_monitoring(trade_pair, buy_exchange, sell_exchange, amount)
        await asyncio.gather(buy_exchange.close(), sell_exchange.close())

async def identify_illiquid_pairs() -> list:
    try:
        illiquid_pairs = []
        available_trade_pairs = [a for a in await get_all_available_trade_pairs([exchange.id for exchange in EXCHANGES]) if re.split("/|:", a)[1] == "USDT"]
        await asyncio.gather(*[exchange.load_markets() for exchange in EXCHANGES])
        tasks = []
        for trade_pair in available_trade_pairs:
            is_illiquid_task = asyncio.create_task(is_illiquid_across_exchanges(trade_pair, EXCHANGES))
            tasks.append(is_illiquid_task)
        # Results is a list of (None | "illiquid_trade_pair") items. Filtering non-None gives illiquid_pairs.
        results = await asyncio.gather(*tasks)
        illiquid_pairs = list(filter(lambda x: x != None, results))
        return illiquid_pairs
    except Exception as e:
        print(f"Exception occured during searching for illiquid pairs --- {e}")

async def calculate_best_spread_and_price(order_books) -> tuple[float]:
    highest_bid_prices = [order_book['bids'][0][0] for order_book in order_books]
    best_bid_price = min(highest_bid_prices)
    lowest_ask_prices = [order_book['asks'][0][0] for order_book in order_books]
    best_ask_price = max(lowest_ask_prices)
    best_spread = (best_ask_price - best_bid_price) / best_bid_price
    return best_spread, best_bid_price

async def is_illiquid_across_exchanges(trade_pair: str, exchanges: list[ccxtpro.Exchange]) -> None | str:
    """
    Checks through available exchanges to determine whether a pair is suitable for illiquid arbitrage.
    Returns the trade_pair if it is illiquid and suitable, otherwise returns None.
    """
    # Fetching order books asynchronously
    order_books = await asyncio.gather(*[fetch_order_book_with_retries(exchange, trade_pair) for exchange in exchanges])
    best_spread, best_price = await calculate_best_spread_and_price(order_books)

    top_8_bids = [order_book['bids'][0:8] for order_book in order_books]
    top_8_asks = [order_book['asks'][0:8] for order_book in order_books]
    volume = 0
    for bids in top_8_bids:
        for bid in bids:
            volume += bid[1] * best_price
    for asks in top_8_asks:
        for ask in asks:
            volume += ask[1] * best_price
    mean_volume = volume / len(order_books)
    if SPREAD_THRESHOLD < best_spread < 0.08  and mean_volume < UPPER_VOLUME_THRESHOLD:
        print(f"Illiquid arbitrageable trade_pair: {trade_pair}")
        return trade_pair
    else:
        return None

async def search_for_illiquid_pairs():
    illiquid_pairs = await identify_illiquid_pairs()
    counter = 0
    while len(illiquid_pairs) == 0:
        print(f"No opportunities found: {counter}")
        illiquid_pairs = await identify_illiquid_pairs()
        counter += 1
    return illiquid_pairs

async def find_opportunity(trade_pair: str) -> tuple:
    """
    This method looks at bid/ask prices of the trade_pair across exchanges and finds the best opportunity.
    It returns the quadruple of (buy_exchange, sell_exchange, transaction_difference, nominal_price).
    """
    
    order_books = await asyncio.gather(*[fetch_order_book_with_retries(exchange, trade_pair) for exchange in EXCHANGES])

    highest_bid_prices = [order_book['bids'][0][0] for order_book in order_books]
    best_bid_price = min(highest_bid_prices)
    best_bid_exchange_index = highest_bid_prices.index(best_bid_price)
    buy_exchange = EXCHANGES[best_bid_exchange_index]

    lowest_ask_prices = [order_book['asks'][0][0] for order_book in order_books]
    best_ask_price = max(lowest_ask_prices)
    best_ask_exchange_index = lowest_ask_prices.index(best_ask_price)
    sell_exchange = EXCHANGES[best_ask_exchange_index]

    nominal_price = (best_bid_price + best_ask_price) / 2
    transaction_difference = (best_ask_price - best_bid_price) / nominal_price * 100
    return (buy_exchange, sell_exchange, transaction_difference, nominal_price)

async def place_limit_orders_with_monitoring(trade_pair: str, buy_exchange: ccxtpro.Exchange, 
                                             sell_exchange: ccxtpro.Exchange, amount: float):
    try:
        await asyncio.gather(buy_exchange.load_markets(), sell_exchange.load_markets())
        base_currency = trade_pair.split('/')[0]
        sell_balance = await check_balance(sell_exchange, base_currency)
        if sell_balance < amount:
            raise Exception(f"|---| Not enough {base_currency} to sell {trade_pair} at {sell_exchange.id} |---|")
        
        buy_order_book = await buy_exchange.fetch_order_book(trade_pair)
        buy_price = buy_order_book['bids'][0][0]
        buy_balance = await check_balance(buy_exchange, 'USDT')
        purchaseable_currency_amount = buy_balance / buy_price
        if purchaseable_currency_amount < amount:
            raise Exception(f"|---| Not enough USDT to buy {trade_pair} at {buy_exchange.id} |---|")
        
        buy = asyncio.create_task(place_smart_buy_order(trade_pair, buy_exchange, amount))
        sell = asyncio.create_task(place_smart_sell_order(trade_pair, sell_exchange, amount))
        _ = await asyncio.gather(buy, sell)
    except:
        print("An error occured during place_limit_orders_with_monitoring().")

async def register_mean_prices() -> dict:
    """This function registers mean prices at exchanges to then calculate random
    profits(from price changes) and arbitrage profits(from the script itself)."""
    mean_prices = {}
    for trade_pair in trade_pairs_in_operation:
        prices = await asyncio.gather(*[fetch_price(exchange, trade_pair) for exchange in EXCHANGES])
        mean_price = sum(prices) / len(prices)
        mean_prices[trade_pair] = mean_price
    return mean_prices

async def register_balance() -> float:
    """This function registers total balance across exchanges to then calculate random
    profits(from price changes) and arbitrage profits(from the script itself)."""
    balances = await asyncio.gather(*[check_balance(exchange, "USDT") for exchange in EXCHANGES])
    total_balance = sum(balances)
    return total_balance

async def place_smart_buy_order(trade_pair: str, buy_exchange: ccxtpro.Exchange, amount: float) -> None:
    global time_of_last_trade
    try:
        await buy_exchange.load_markets()

        buy_order_book = await buy_exchange.fetch_order_book(trade_pair)
        buy_price = buy_order_book['bids'][0][0]
        buy_balance = await check_balance(buy_exchange, 'USDT')
        purchaseable_currency_amount = buy_balance / buy_price
        if purchaseable_currency_amount < amount:
            raise Exception(f"|---| Not enough USDT to buy {trade_pair} at {buy_exchange.id} |---|")

        await asyncio.sleep(2)
        # Place orders simultaneously
        buy_order = await buy_exchange.create_limit_buy_order(trade_pair, amount, buy_price)

        print(f"Placed smart limit buy order for {amount} {trade_pair} at {buy_price} on {buy_exchange.id}")
        print()

        # Flags to track order status
        buy_filled = False
        timeout = 15 * 60  # 15 minutes
        start_time = time.time()

        while not buy_filled:
            # Check timeout and market prices
            if time.time() - start_time >= timeout:
                if not buy_filled:
                    # Update buy order price to current bid
                    buy_order_book = await buy_exchange.fetch_order_book(trade_pair)
                    new_buy_price = buy_order_book['bids'][0][0]
                    remaining_amount = await handle_order_cancellation(buy_exchange, buy_order, trade_pair)
                    if remaining_amount == 0:
                        print(f"Buy order for {trade_pair} at {buy_exchange} filled fully. Remaining amount = 0.")
                        buy_filled = True
                        continue
                    buy_order = await buy_exchange.create_limit_buy_order(trade_pair, remaining_amount, new_buy_price)
                    print(f"Timeout reached. Updated buy order price to {new_buy_price} for {trade_pair} at {buy_exchange}")

                # Reset timeout timer
                start_time = time.time()

            # Check each order's status
            if not buy_filled:
                buy_order_status = await get_order(buy_order, trade_pair, buy_exchange)
                if buy_order_status['status'] == 'closed':
                    buy_filled = True
                    time_of_last_trade[trade_pair] = time.time()
                    print(f"{trade_pair} buy order filled at {buy_order_status['price']}")

            # Allow other tasks to execute
            await asyncio.sleep(5)

        print(f"Buy order completed for {trade_pair} at {buy_exchange}.")

    except Exception as e:
        await handle_order_cancellation(buy_exchange, buy_order, trade_pair)
        raise Exception(f"Error during limit buy order transactions: {e}")

async def place_smart_sell_order(trade_pair: str, sell_exchange: ccxtpro.Exchange, amount: float) -> None:
    global time_of_last_trade
    try:
        await sell_exchange.load_markets()

        sell_order_book = await sell_exchange.fetch_order_book(trade_pair)
        sell_price = sell_order_book['asks'][0][0]
        # Checking that there is enough coins to sell
        base_currency = trade_pair.split('/')[0]
        sell_balance = await check_balance(sell_exchange, base_currency)
        if sell_balance < amount:
            raise Exception(f"|---| Not enough {base_currency} to sell {trade_pair} at {sell_exchange.id} |---|")

        await asyncio.sleep(2)
        sell_order = await sell_exchange.create_limit_sell_order(trade_pair, amount, sell_price)

        print(f"Placed smart limit sell order for {amount} {trade_pair} at {sell_price} on {sell_exchange.id}")
        print()

        # Flags to track order status
        sell_filled = False
        timeout = 15 * 60  # 15 minutes
        start_time = time.time()

        while not sell_filled:
            # Check timeout and market prices
            if time.time() - start_time >= timeout:
                if not sell_filled:
                    # Update sell order price to current ask
                    sell_order_book = await sell_exchange.fetch_order_book(trade_pair)
                    new_sell_price = sell_order_book['asks'][0][0]
                    remaining_amount = await handle_order_cancellation(sell_exchange, sell_order, trade_pair)
                    if remaining_amount == 0:
                        print(f"Sell order for {trade_pair} at {sell_exchange} filled fully. Remaining amount = 0.")
                        sell_filled = True
                        continue
                    sell_order = await sell_exchange.create_limit_sell_order(trade_pair, remaining_amount, new_sell_price)
                    print(f"Timeout reached. Updated sell order price to {new_sell_price} for {trade_pair} at {sell_exchange}")

                # Reset timeout timer
                start_time = time.time()

            if not sell_filled:
                sell_order_status = await get_order(sell_order, trade_pair, sell_exchange)
                if sell_order_status['status'] == 'closed':
                    sell_filled = True
                    time_of_last_trade[trade_pair] = time.time()
                    print(f"{trade_pair} sell order filled at {sell_order_status['price']}")

            # Allow other tasks to execute
            await asyncio.sleep(10)  # Adjust delay as necessary

        print(f"Sell order completed for {trade_pair} at {sell_exchange}.")

    except Exception as e:
        await handle_order_cancellation(sell_exchange, sell_order, trade_pair)
        raise Exception(f"Error during limit sell order transactions: {e}")

@exponential_backoff
async def check_balance(exchange: ccxtpro.Exchange, currency: str) -> float:
    """Fetch the available balance for a specific currency."""
    balance = await exchange.fetch_balance()
    return balance['free'].get(currency, 0)  # Only free balance is considered

@exponential_backoff
async def check_spread(exchange1: ccxtpro.Exchange, exchange2: ccxtpro.Exchange, trade_pair: str) -> float:
    """Checks spread on `trade_pair` across `exchange1` and `exchange2`"""
    price1, price2 = await asyncio.gather(
        fetch_price_rest(exchange1, trade_pair), fetch_price_rest(exchange2, trade_pair)
    )
    mean = (price1 + price2) / 2
    spread = abs(price2 - price1) / mean
    return spread

async def catch_illiquid_pairs_in_operation() -> list[str]:
    """Awaits for at least some trade pairs in operation to become suitable again."""
    pairs = [trade_pair for trade_pair in trade_pairs_in_operation if await is_illiquid_across_exchanges(trade_pair, EXCHANGES)]
    while pairs == []:
        pairs = [trade_pair for trade_pair in trade_pairs_in_operation if await is_illiquid_across_exchanges(trade_pair, EXCHANGES)]
        await asyncio.sleep(5)
    return pairs

@exponential_backoff
async def fetch_price_rest(exchange: ccxtpro.Exchange, trade_pair: str) -> float:
    # Load markets before using the exchange
    try:
        ticker = await exchange.fetch_ticker(trade_pair)
        return ticker['last'] if ticker else None
    except Exception as e:
        print(f"Fetching price error: {e}")
        return None
    
async def execute_rebalancing(exchange1: ccxtpro.Exchange, exchange2: ccxtpro.Exchange, trade_pair: str) -> None:
    print(">>>>>> Executing rebalancing >>>>>>")
    spread = await check_spread(exchange1, exchange2, trade_pair)
    while spread > 0.015:
        await asyncio.sleep(10)
        spread = await check_spread(exchange1, exchange2, trade_pair)
        print(f"Spread across {exchange1}, {exchange2} for {trade_pair} === {spread}")
    return await rebalance(exchange1, exchange2, trade_pair) # TODO check _ = 

@exponential_backoff
async def handle_order_cancellation(exchange: ccxtpro.Exchange, order, trade_pair: str) -> float:
    """Handles order cancellation and returns the remaining amount."""
    order_id = order['id']
    order_symbol = order['symbol']
    status: dict = await get_order(order, trade_pair, exchange)
    remaining_amount = status.get('remaining', 0)
    cancellation = await exchange.cancel_order(order_id, order_symbol)
    return remaining_amount

async def calculate_half_the_lowest_amount(exchange1: ccxtpro.Exchange, exchange2: ccxtpro.Exchange, currency: str) -> float:
    balance1, balance2 = await asyncio.gather(check_balance(exchange1, currency), check_balance(exchange2, currency))
    amount = min(balance1, balance2) / 2
    if amount != 0:
        amount = round_3_sig_fig(amount)
    return amount

async def open_parallel_positions(trade_pair: str, exchanges: list[ccxtpro.Exchange], amount: float) -> None:
    """
    Buys some quantity of trade_pair for leveraging it later using limit order with monitoring.
    """
    tasks = []
    for exchange in exchanges:
        task = asyncio.create_task(place_smart_buy_order(trade_pair, exchange, amount))
        tasks.append(task)
    _ = await asyncio.gather(*tasks)
    print(f"Opened parallel positions at {exchanges} for {trade_pair}")

async def close_positions(trade_pair: str, exchanges: list[ccxtpro.Exchange]) -> None:
    """
    Some some quantity of trade_pair to close the position with monitoring.
    """
    tasks = []
    for exchange in exchanges:
        coin = trade_pair.split('/')[0]
        amount = await check_balance(exchange, coin)
        if amount != 0:
            task = asyncio.create_task(place_smart_sell_order(trade_pair, exchange, amount))
            tasks.append(task)
    _ = await asyncio.gather(*tasks)
    print(f"|---|Closing {trade_pair} limit orders completed!|---|")

async def close_all_positions() -> None:
    closes = [close_positions(trade_pair, EXCHANGES) for trade_pair in trade_pairs_in_operation]
    _ = await asyncio.gather(*closes)

@exponential_backoff
async def fetch_order_book_with_retries(exchange, trade_pair):
    return await exchange.fetch_order_book(trade_pair)

@exponential_backoff
async def get_order(order, trade_pair: str, exchange: ccxtpro.Exchange):
    if exchange.id == 'bybit':
        return await exchange.fetch_open_order(order['id'], trade_pair)
    else:
        return await exchange.fetch_order(order['id'], trade_pair)

async def rebalance(exchange1, exchange2, trade_pair) -> None:
    """When half of the minimum asset is lower than 1 USDT, complete a rebalance, selling on one account
    and buying on the other, so that the assets are again equal. Wait for low spread, say lower than 0.5% and
    then execute rebalancing."""
    print(f"Executing rebalancing for {trade_pair} at {exchange1} and {exchange2}...")
    try:
        currency = trade_pair.split('/')[0]
        balance1, balance2 = await asyncio.gather(
            check_balance(exchange1, currency), check_balance(exchange2, currency)
        )
        delta_balance1 = (balance2 - balance1) / 2
        delta_balance2 = (balance1 - balance2) / 2
        orders = []
        if delta_balance1 >= 0:
            orders.append(
                place_smart_buy_order(trade_pair, exchange1, delta_balance1)
            )
        else:
            orders.append(
                place_smart_sell_order(trade_pair, exchange1, abs(delta_balance1))
            )
        if delta_balance2 >= 0:
            orders.append(
                place_smart_buy_order(trade_pair, exchange2, delta_balance2)
            )
        else:
            orders.append(
                place_smart_sell_order(trade_pair, exchange2, abs(delta_balance2))
            )
        print(f"Rebalance orders: {orders}")
        return await asyncio.gather(*orders) # TODO check _ = 
    except Exception as e:
        print(f"Rebalancing error: {e}")

async def close_exchanges() -> None:
    await asyncio.gather(*[exchange.close() for exchange in EXCHANGES])

async def conclude_arbitrage_session(start_total_balance: float, start_mean_prices: dict):
    end_total_balance = await register_balance()
    end_mean_prices = await register_mean_prices()

    pprint(end_total_balance)
    pprint(start_total_balance)
    pprint(end_mean_prices)
    pprint(start_mean_prices)

    net_balance_change = end_total_balance - start_total_balance
    random_asset_change = 0
    for trade_pair in trade_pairs_in_operation:
        start_mean_price = start_mean_prices[trade_pair]
        end_mean_price = end_mean_prices[trade_pair]
        random_asset_change += 2 * open_amount_USDT * (end_mean_price - start_mean_price)
    arbitrage_profit_with_fees = net_balance_change - random_asset_change

    print("\n|-----|")
    print(f"The net balance change === {round_3_sig_fig(net_balance_change)} USDT")
    print(f"The random_asset_change due to price changes === {round_3_sig_fig(random_asset_change)} USDT")
    print(f"Pure arbitrage profits + fees === {round_3_sig_fig(arbitrage_profit_with_fees)} USDT")
    print("|-----|\n")

def finish() -> bool:
    q = queue.Queue()

    # Thread target function to capture input
    def get_input():
        q.put(input("Enter 'q' within 5 seconds: "))

    # Start input thread
    input_thread = threading.Thread(target=get_input)
    input_thread.start()

    try:
        # Wait for input or timeout after 5 seconds
        input_value = q.get(timeout=5)
        return input_value == 'q'
    except queue.Empty:
        # Timeout occurred before any input was received
        print()
        return False

def initialise_exchange(exchange_id) -> ccxtpro.Exchange:
    exchange: ccxtpro.Exchange = getattr(ccxtpro, exchange_id)({
        'apiKey': KEYS[exchange_id]['apiKey'],
        'secret': KEYS[exchange_id]['secret'],
        'password': KEYS[exchange_id]['password']
        })
    return exchange

async def load_exchanges():
    await asyncio.gather(*[exchange.load_markets() for exchange in EXCHANGES])

async def test():
    ...

async def main():
    """
    There are zero USDT transfer fees from bybit to kucoin. TON: bybit -> kucoin; BEC20: kucoin -> bybit.
    """
    try:
        await load_exchanges()

        global start_total_balance
        start_total_balance = await register_balance()

        while True:
            _ = await arbitrage(operate)
            await asyncio.sleep(5)
            print(f"Trade pairs in operation: {trade_pairs_in_operation}.")
            print("Arbitrage round complete.")

    except: # (KeyboardInterrupt, asyncio.exceptions.CancelledError)
        print("\nShutting down... Performing cleanup tasks.")
        await close_all_positions()
        await conclude_arbitrage_session(start_total_balance, start_mean_prices)
        await close_exchanges()

    finally:
        print("|---|\nArbitrage session successful!\n|---|")

if __name__ == "__main__":
    print(">>>>>>------------<<<<<<")
    print(">>>>>>---LAUNCH---<<<<<<")
    print(">>>>>>------------<<<<<<")

    # Load API keys
    with open("keys.json", "r") as f:
        KEYS = json.load(f)
        f.close()

    # Initialise global variables
    trade_pairs_in_operation = []
    time_of_last_trade = {}
    start_total_balance = 0
    start_mean_prices = {}
    open_amount_USDT = 6

    # Initialise constants
    bybit = initialise_exchange('bybit')
    kucoin = initialise_exchange('kucoin')
    EXCHANGES = [bybit, kucoin]
    MY_EXCHANGES = ['bybit', 'okx', 'kucoin']
    SPREAD_THRESHOLD = 0.03
    UPPER_VOLUME_THRESHOLD = 10000
    TRADE_PAIRS_LIMIT = 2
    TIME_THRESHOLD = 12 * 60 * 60

    # The main script
    asyncio.run(main())

    print(">>>>>>------------<<<<<<")
    print(">>>>>>---FINISH---<<<<<<")
    print(">>>>>>------------<<<<<<")
# Connection closed by remote server, closing code 1006