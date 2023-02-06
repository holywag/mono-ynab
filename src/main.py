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
import monobank
import configuration_parser

class FakeConfiguration:
    def __init__(self, file_path):
        self.file_path = file_path
    
    def __repr__(self):
        return f'FakeConfiguration("{self.file_path}")'

class FakeStmt:
    def __init__(self, stmt, conf):
        self.conf = conf
        self.stmt = stmt
    
    def __repr__(self):
        return f'FakeStmt(conf="{self.conf}", stmt="{self.stmt}")'

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

    def on_error(self, error):
        logging.error(f'UploadObserver: on_error({error})')

class RequestEngine:
    def __init__(self):
        self.observers = []

    def add_transaction_observer(self, observer):
        self.observers.append(observer)

    async def __request_statements(self, configuration):
        # TODO: implement real logic of request
        # mono_api = monobank.MonobankApi(monobank.ApiClient(token))
        # TODO: add configuration to statement/transaction objects
        sleep_duration = random.randrange(1, 10)
        logging.info(f'request_statements: sleep_duration={sleep_duration} configuration={configuration}')
        await asyncio.sleep(sleep_duration)
        logging.info(f'request_statements: configuration={configuration} DONE')
        return rx.from_list([
            FakeStmt('stmt1', configuration),
            FakeStmt('stmt2', configuration),
            FakeStmt('stmt3', configuration)])

    async def __main(self, configs_observable: rx.Observable):
        configs_observable \
            .pipe(
                op.flat_map(lambda conf: rx.from_future(asyncio.create_task(self.__request_statements(conf)))),
                op.merge_all(),
                # op.retry(3),
                # # op.filter(filters.TransferFilter()),
                # # op.map(model.ynab_transaction.YnabTransactionConverter()),
                *(op.do(o) for o in self.observers)
            ) \
            .subscribe(
                scheduler=AsyncIOScheduler(asyncio.get_running_loop())
            )

    def run(self, configs_observable: rx.Observable):
        logging.info('Creating loop')
        loop = asyncio.new_event_loop()
        loop.create_task(self.__main(configs_observable))
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
    engine.run(configuration_parser.from_filesystem('./configurations'))
