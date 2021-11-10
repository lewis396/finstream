import asyncio
import contextlib
from collections import defaultdict
from cryptofeed.callback import TradeCallback
from cryptofeed import FeedHandler
from cryptofeed.exchanges.bitfinex import Bitfinex
# from cryptofeed import BITMEX, BITSTAMP, BITFINEX, GDAX
from cryptofeed.defines import TRADES
import datetime
from dtypes import StaticDict
import inspect
import json
import logging
import websockets
import time
from typing import Iterable, List, Union

PROTOCOL_WEBSOCKETS = 'websockets'
EXCHANGE_BITFINEX = 'BITFINEX'

# For ease of indexing (for maps etc.)
TRADES = 'trades'

logging.basicConfig(level=logging.INFO, format='%(levelname)s :: %(asctime)s : %(message)s')
INFO = logging.info

# from qpython import qconnection


# In q instance open the port: \p 5002
# q = qconnection.QConnection(host='localhost', port=5002, pandas=True)
# q.open()

# Callback - a q script that creates the default table
# q.sendSync("""trades:([]systemtime:`datetime$();
#        side:`symbol$();
#        amount:`float$();
#        price:`float$();
#        exch:`symbol$())""")

async def trade(feed, pair, id=None, timestamp=None, side=None, amount=None, price=None, receipt_timestamp=0.0):
    print('Feed: {} Pair: {} System Timestamp: {} Amount: {} Price: {} Side: {}'.format(
        feed, pair, localtime, amount, price, side,
    ))
    # q.sendSync('`trades insert(.z.z;`{};{};{};`{})'.format(
    #    side,
    #    float(amount),
    #    float(price),
    #    str(feed)
    # ))


def main_example():
    f = FeedHandler()
    # f.add_feed(Bitmex(symbols=['BTC-USD'], channels=[TRADES], callbacks={TRADES: TradeCallback(trade)}))
    # f.add_feed(Bitstamp(symbols=['BTC-USD'], channels=[TRADES], callbacks={TRADES:TradeCallback(trade)}))
    f.add_feed(Bitfinex(symbols=['BTC-USD'], channels=[TRADES], callbacks={TRADES: TradeCallback(trade)}))
    # f.add_feed(GDAX(pairs=['BTC-USD'], channels=[TRADES], callbacks={TRADES:TradeCallback(trade)}))
    f.run()


async def internal():
    test_req = json.dumps({"event": "conf", "flags": 65536})
    async with websockets.connect("wss://api.bitfinex.com/ws/2") as websocket:

        await websocket.send(test_req)
        print("Successful connection")
        await websocket.send(json.dumps({
            "event": "subscribe",
            "channel": "trades",
            "pair": "tBTCUSD",
        }))

        async def sleep_indefinitely():
            """ Watcher

            If the connection to the stream is open and running either:
                close the connection if interarrival time is exceeded
            OR
                continue to wait.

            Add this to the event loop and run it indefinitely
            """
            while True:
                await asyncio.sleep(1)

        # Handler
        # TODO Read from the connection. If there is nothing there return. Otherwise process
        async for msg in websocket:
            if msg:
                print(msg)
            else:
                asyncio.wait(1)
        # print(resp)
        # print(resp2)
    print("Success")


class ClientPool:
    """ Supervises the event loop and manages state around the loop.

    """

    def __init__(self, session_timeout=45, cache_size=20, cache_type='fixed') -> None:
        self.session_timeout: int = session_timeout
        self.cache_size: int = cache_size
        self.cache_type: str = cache_type
        self.clients: list = []
        self.all_work: list = []

    def add_client(self, new_client):
        self.clients.append(new_client)
        return

    def configure_internal_work(self):
        for client in self.clients:
            per_client_work = getattr(client, 'start_streaming')
            self.all_work.append(per_client_work)

    def do_internal_work(self, loop):
        for local_work in self.all_work:
            if inspect.iscoroutinefunction(local_work):
                loop.create_task(local_work())

    def do_work(self):
        """ Complete all outstanding work.

        The work to be done is encapsulated in the
        :return:
        """
        loop = asyncio.get_event_loop()
        self.do_internal_work(loop)
        loop.run_forever()


class Callback:

    def __init__(self, func):
        """ Handle an incoming message.

        :param func: Callable
            A wrapped function.
        """
        self.func = func
        self.awaitable = inspect.iscoroutinefunction(func)

    async def __call__(self, time_id, *args, **kwargs):
        if args is None:
            return
        elif self.awaitable:
            await self.func(time_id, *args, **kwargs)
        else:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.func, (time_id, args, kwargs))


class ExchangeClient:
    pass


class BitfinexClient(ExchangeClient):
    """ A single stream of data from the Bitfinex exchange.

    A stream is defined by an exchange, a channel and a symbol.
    """
    name = EXCHANGE_BITFINEX
    protocol = PROTOCOL_WEBSOCKETS
    endpoint = "wss://api.bitfinex.com/ws/2"
    flags = 65536

    def __init__(self, pairs=None, *, channels=None, symbol='BTCUSD', setup_dict={}):
        """
        :param pairs:
            An iterable with channel-value pair elements.
        :param channels:
            One channel, or a list of channels.
        :param symbol:

        :param setup_dict:
        """

        self.configs: List[defaultdict] = []
        self.callbacks = dict.fromkeys(['trades'])

        self.callbacks_ = StaticDict(
            [TRADES],
            default=Callback(None)
        )

        self.chan2chanId = dict.fromkeys([TRADES], 0)
        self.chanId2chan = dict.fromkeys(map(str, self.chan2chanId.values()), '')
        self.chanId2chan.update(**{str(v): k for k, v in self.chan2chanId.items()})

        if pairs:
            if isinstance(pairs, tuple):
                try:
                    channel, symbol, callbacks = pairs
                    self.channels, self.symbols, self.callbacks = [channel], [symbol], [callbacks]
                    # for channel,
                except ValueError as e:
                    print(f'Expected single tuple of form (CHANNEL, SYMBOL) got {pairs}.')
                    raise e
            elif isinstance(pairs, list):
                if not all((isinstance(pair, tuple) for pair in pairs)):
                    raise TypeError('Expected a list of tuples')
                # TODO Apply DRY here.
                self.channels, self.symbols, self.callbacks = list(map(list, zip(*pairs)))
        else:

            if channels and symbol:
                if isinstance(channels, str):
                    self.channels = [channels]
                elif not isinstance(channels, Iterable):
                    raise TypeError(f'channels must be either a sinlge string or an iterable with string elements.')

                self.symbol = 't' + symbol

        self.requests = dict(
            configure={'event': 'conf', 'flags': self.flags},
            subscribe={'event': 'subscribe', 'channel': 'trades', 'pair': 'tBTCUSD'},
        )

    def add_callback(self, callback, channel=TRADES):
        self.callbacks_.update({channel: callback})

    async def handle_incoming(self, connection):
        """ Handle incoming messages to the client.

        TODO General refactor. Want json loads output to tuple of args
             Or perhaps we start the callback here and focus on calling that
             for either the incoming message or each of the messages in a
             list of messages, as arises in the case of a snapshot.
        :param connection:
            THe connection to which we listen and accept messages.
        :return: tuple
        """
        async for message in connection:
            # We may recieve
            if message:
                # If string message is a heartbeat continue
                # TODO This is horribly inefficient. Try to avoid this.
                if message.find('hb') != -1:
                    continue
                msg_json = json.loads(message)
                if isinstance(msg_json, dict):
                    self.handle_event(msg_json)
                elif isinstance(msg_json, list):
                    # print(msg_json)
                    # trades channel update
                    if len(msg_json) == 4:
                        chan_id, msg_type, (seq_no, trade_id, price, volume), order = msg_json
                        cb = self.callbacks[0].get(TRADES)
                        await cb(self.chanId2chan[str(chan_id)], msg_type, seq_no, trade_id, price, volume, order)
                    # trade snapshot
                    if len(msg_json) == 3:
                        msg_type = 'ss'
                        print('\tStart of Initial Snapshot')
                        chan_id, trades_list, order = msg_json
                        for (seq_no, trade_id, price, volume) in trades_list:
                            await self.callbacks[0].get(TRADES)(self.chanId2chan[str(chan_id)], msg_type, seq_no, trade_id, price,
                                                                volume, order)
                        print('\tEnd of Initial Snapshot')
                else:
                    # Log unexpected input -- flag depending on user preferences
                    asyncio.wait(1)
            else:
                # Wait for messages to arrive.
                asyncio.wait(1)

    def handle_event(self, event_dict: dict):
        """ Handle incoming messages that convert to a dict with key 'event'.

        There are, so far, three events handled by this function. We may receive a
        dict where...

            `event_dict.get('event') == 'info'`
        For example:
            `{'event': 'info',
              'version': 2,
              'serverId': '87eab24b-bc86-4169-a08a-c49957c23ad7',
              'platform': {'status': 1}}`

            `event_dict.get('event') == 'conf'`
        For example:
            `{'event': 'conf', 'status': 'OK', 'flags': 65536}}`

            `event_dict.get('event') == 'subscribed'`
        For example:
            `{'event': 'subscribed',
              'channel': 'trades',
              'chanId': 35,
              'symbol': 'tBTCUSD',
              'pair': 'BTCUSD'}`

        :param event_dict:
        :return:
        """
        switch = event_dict.get('event', None)
        if switch is None:
            raise Exception('Error: anticipated event did not occur.')

        if switch == 'info':
            # Extract status and raise an exception if need be.
            status = event_dict.get('platform').get('status')
            if status != 1:
                raise ConnectionError(f'Unable to connect to server {self.endpoint!r}')

        elif switch == 'conf':
            # Extract status and flags.
            status = event_dict.get('status')
            flags = event_dict.get('flags')
            if status != 'OK':
                raise ConnectionError(f'Error connection to server {self.endpoint!r}')
            if flags != self.flags:
                raise ValueError(f'expected flags=={self.flags}: got flags == {flags}')

        elif switch == 'subscribed':
            channel, symbol, id = (event_dict.get(k) for k in ['channel', 'symbol', 'chanId'])
            # Associate the id to the channel name
            self.update_channel_id_associations(channel, id)
            print(f'Subscription Success')
            print(f'\tchannel: {channel} symbol: {symbol} channel ID: {id}')

        else:
            raise KeyError(f'Event {switch!r} is unsupported.')

    async def start_streaming(self):
        """ Setup a websocket connection and manage all communications across that channel.

        TODO Support multiple symbols/tickers on the same connection.

        TODO Strip out the connection, setup and handler functionality.

        TODO Maybe add some sort of watcher that should run in the background. E.g. could flag strange data.

        TODO Refactor to use a context manager.
        :return:
        """
        # Open the connection.
        #   The response will contain the mapping `event="info"`.
        async with websockets.connect(self.endpoint) as ws_connection:
            # Send the configuration request.
            #   The response will contain the mapping `event="conf"`.
            await ws_connection.send(json.dumps(self.requests.get('configure')))

            # Send the  subscription requests
            #   The responses all will contain the mapping `event="subscribed"`.
            for channel, symbol in zip(self.channels, self.symbols):
                # One subscription per combination of exchange, channel, symbol.
                subscription_request = dict(event='subscribe', channel=channel, symbol=symbol)
                await ws_connection.send(json.dumps(subscription_request))

            # TODO Create a listener here to monitor activity / inactivity.

            # Handle incoming messages.
            await self.handle_incoming(ws_connection)

    def update_channel_id_associations(self, channel, channel_id):
        """ Simultaneously update the channel to channel id map and it's inverse. """
        self.chan2chanId.update(**{channel: channel_id})
        self.chanId2chan.update(**{str(channel_id): channel})


async def callback_function(channel: str,
                            msg_type: str,
                            seq_no: int,
                            trade_id: int,
                            price: float,
                            amount: float,
                            ordering: int):
    """ Write a parsed trade to console.
    :param timestamp: float
        A timestamp, the time since the start of the epoch until the arrival of the message.
    :param chan_id:
        The channel
    :param msg_type:
        Either "tu" or "te".
    :param seq_no:
        Unique integer identifying the trade.
    :param trade_id:
        Unique integer identifying the trade.
    :param price:
        The price at which the trade was struck.
    :param amount:
        The size of the reported trade.
    :param ordering:
        Unique integer imposing ordering on the trades data.
    """
    INFO(f'{channel} \t[seq: {seq_no}  id: {trade_id} price: {price} amount: {amount}]')


def run_loop():
    loop = asyncio.get_event_loop()
    loop.create_task(internal())
    loop.run_forever()


if __name__ == '__main__':
    pool = ClientPool()
    client = BitfinexClient([('trades', 'tBTCUSD', {TRADES: Callback(callback_function)}),
                             ('trades', 'tETHUSD', {TRADES: Callback(callback_function)})],
                            channels="trades", symbol="BTCUSD")
    client.add_callback(callback_function, TRADES)
    pool.add_client(client)
    pool.configure_internal_work()
    pool.do_work()
