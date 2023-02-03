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

class RequestEngine:
    def __init__(self):
        self.handlers = []

    def add_transaction_handler(self, handler):
        self.handlers.append(handler)

    def __execute_handlers(self, t):
        for handler in self.handlers:
            handler(t)

    @staticmethod
    def __async_call(task_func):
        def __call_async_func(*args, **kwargs):
            return rx.from_future(asyncio.create_task(task_func(*args, **kwargs)))
        return __call_async_func

    async def __request_statements(self, configuration):
        # TODO: add configuration to statement/transaction objects
        pass

    async def run(self, configs_dir):
        rx.from_iterable(os.listdir(configs_dir)) \
            .pipe(
                op.map(lambda p: os.path.join(configs_dir, p)),
                op.filter(os.path.isdir),
                op.map(model.configuration.Configuration),
                op.flat_map(self.__async_call(self.__request_statements)),
                op.retry(3),
                op.map(model.monobank_statement.BankStatement),
                op.filter(filters.TransferFilter()),
                op.map(model.ynab_transaction.YnabTransactionConverter()),
                op.do_action(on_next = self.__execute_handlers),
            ) \
            .subscribe(
                on_next=lambda trans: asyncio.ensure_future(send_transactions(trans)),
                on_completed=lambda: await asyncio.sleep(10),
                scheduler=AsyncIOScheduler(asyncio.get_event_loop())
            )

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

    # loop the scheduler
    asyncio.run(main(importers))
    