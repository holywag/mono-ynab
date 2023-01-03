import time
import asyncio
import logging
import rx
from rx import operators as op
from rx.scheduler.eventloop import AsyncIOScheduler
import os
import model
import filters

def request_statements(conf):
    # TODO: add configuration to statement/transaction objects
    return rx.from_future(asyncio.create_task(request_statements(conf)))

async def send_transactions(transactions):
    pass

async def main(configs_dir):
    while True:
        rx.from_iterable(os.listdir(configs_dir)) \
            .pipe(
                op.map(lambda p: os.path.join(configs_dir, p)),
                op.filter(os.path.isdir),
                op.map(model.configuration.Configuration),
                op.flat_map(request_statements),
                op.retry(3),
                op.map(model.monobank_statement.BankStatement),
                op.filter(filters.TransferFilter()),
                op.map(model.ynab_transaction.YnabTransactionConverter()),
                op.buffer_with_count(100)
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
    