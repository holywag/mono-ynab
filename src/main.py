import time
import asyncio
import logging
import rx
from rx import operators as op
from rx.scheduler.eventloop import AsyncIOScheduler
import os
import model
import filters
import itertools
import random
import traceback
import typing
import configuration_parser
from pathlib import Path
from datetime import datetime
from model import bank_statement
import json
import monobank_api

class UploadObserver(rx.typing.Observer):
    def __init__(self):
        self.transactions = []

    async def send_transactions(self):
        sleep_duration = random.randrange(2, 5)
        logging.info(f'UploadObserver: send_transactions sleep_duration={sleep_duration} n={len(self.transactions)} {self.transactions}')
        await asyncio.sleep(sleep_duration)
        logging.info(f'UploadObserver: send_transactions DONE')

    def on_next(self, transaction):
        logging.debug(f'UploadObserver on_next {transaction}')
        self.transactions.append(transaction)
    
    def on_completed(self):
        logging.debug(f'UploadObserver on_completed')
        asyncio.create_task(self.send_transactions())

    def on_error(self, error: Exception):
        logging.error(f'UploadObserver: on_error({error.__class__.__name__}: {error})')
        traceback.print_tb(error.__traceback__)

class RequestEngine:
    def __init__(self):
        self.observers = []

    def add_transaction_observer(self, observer):
        self.observers.append(observer)

    async def __main(self):
        configuration_parser.from_filesystem(Path('./configurations')) \
            .pipe(
                # TODO: calculate new time range, implement overriding
                op.flat_map(monobank_api.from_configuration),
                # op.retry(3),
                # TODO: adapt transfer filter
                # # op.filter(filters.TransferFilter()),
                # TODO: adapt transaction converter
                # # op.map(model.ynab_transaction.YnabTransactionConverter()),
                *(op.do(o) for o in self.observers)
            ) \
            .subscribe(
                # TODO: store the updated time range configuration
                scheduler=AsyncIOScheduler(asyncio.get_running_loop())
            )

    def run(self):
        logging.info('Creating loop')
        loop = asyncio.new_event_loop()
        loop.create_task(self.__main())
        aws = asyncio.all_tasks(loop)
        logging.info('Running loop tasks')
        while len(aws) > 0:
            loop.run_until_complete(asyncio.gather(*aws))
            aws = asyncio.all_tasks(loop)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    engine = RequestEngine()
    engine.add_transaction_observer(UploadObserver())
    engine.run()
