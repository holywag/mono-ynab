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
from ynab_api_wrapper import YnabApiWrapper, SingleBudgetYnabApiWrapper, YnabTransactionFields
from collections import defaultdict

def send_transactions(scheduler = rx.typing.Scheduler) -> rx.typing.Observer[model.bank_statement.BankStatement]:
    rx.pipe(
        op.group_by(lambda s: s.configuration.path)
    )


    __ynab_api = None
    def ynab_api():
        global __ynab_api
        if not __ynab_api:
            __ynab_api = SingleBudgetYnabApiWrapper(YnabApiWrapper(config.token), config.budget_name)
        return __ynab_api

    subject = rx.subject.Subject()
    ynab_api_observable = subject.pipe(
            op.)
     subject.pipe
        .pipe(
            op.first()
            op.skip(1)
            op.zip(rx.repeat_value())
        )
    subject.subscribe(scheduler=scheduler)
    return subject


class UploadObserver(rx.typing.Observer[model.bank_statement.BankStatement]):
    """
    An instance of this observer works on a single configuration, i.e.
    it will remember ynab settings of the initial transaction and apply
    them to all further transactions.
    """

    def __init__(self):
        self.transactions = []
        self.__ynab_api = None
        logging.info('UploadObserver is created')

    def ynab_api(self, config: model.configuration.YnabImportSettings) -> SingleBudgetYnabApiWrapper:
        if not self.__ynab_api:
            self.__ynab_api = SingleBudgetYnabApiWrapper(YnabApiWrapper(config.token), config.budget_name)
        return self.__ynab_api

    async def send_transactions(self):
        logging.info(f'TODO: send_transactions n={len(self.transactions)} {self.transactions}')

    def on_next(self, stmt: bank_statement.BankStatement):
        logging.debug(f'UploadObserver on_next stmt={stmt}')
        ynab_api = self.ynab_api(stmt.configuration.ynab)
        field_config = stmt.configuration.statement_field_settings
        ynab_fields = YnabTransactionFields(
            account_id=ynab_api.get_account_id_by_name(stmt.account.ynab_name),
            date=stmt.time.date(),
            amount=stmt.amount*10,
            payee_name=field_config.payee_aliases_by_payee_regex.get(stmt.description, stmt.description),
        )
        category = field_config.categories_by_payee_regex.get(stmt.description, field_config.categories_by_mcc.get(stmt.mcc))
        if category:
            ynab_fields.category_id = ynab_api.get_category_id_by_name(category.group, category.name)
        if stmt.transfer_account:
            ynab_fields.payee_id = ynab_api.get_transfer_payee_id_by_account_name(
                stmt.transfer_account.ynab_name)
        if not ynab_fields.category_id and not ynab_fields.payee_id:
            ynab_fields.memo = f'mcc: {stmt.mcc} description: {stmt.description}'
        logging.debug(f'UploadObserver on_next fields={ynab_fields}')
        self.transactions.append(ynab_fields)
    
    def on_completed(self):
        logging.info(f'UploadObserver on_completed')
        # TODO await
        asyncio.create_task(self.send_transactions())

    def on_error(self, error: Exception):
        logging.error(f'UploadObserver: on_error({error.__class__.__name__}: {error})')

class TimeRangeCalculator(rx.typing.Observer[model.configuration.Configuration]):
    def __init__(self, override_start: datetime = None, override_end: datetime = None):
        self.override_start = override_start
        self.override_end = override_end
        self.configs = []

    def on_next(self, configuration: model.configuration.Configuration):
        time_range = configuration.bank.time_range
        if self.override_start:
            time_range.start = self.override_start
        elif time_range.end:
            time_range.start = time_range.end
        elif not time_range.start:
            raise ValueError('Cannot calculate new time range start: neither end time is set, nor override start time is proveded.')
        time_range.end = self.override_end and self.override_end or datetime.now(tz=time_range.start.tzinfo)
        if time_range.end < time_range.start:
            raise ValueError(
                f'Invalid time range settings: start({time_range.start}) is later then end({time_range.end})')
        self.configs.append(configuration)
    
    def on_completed(self):
        logging.debug('TimeRangeCalculator on_completed')
        # TODO
        logging.info(f'TODO: store the following {len(self.configs)} configurations: {self.configs}')

    def on_error(self, error):
        logging.error(f'TimeRangeCalculator: on_error({error.__class__.__name__}: {error})')

class RequestEngine:
    def __init__(self):
        self.observers = []

    def add_group_observer(self, observer):
        self.observers.append(observer)

    async def run(self):
        scheduler = AsyncIOScheduler(asyncio.get_running_loop())
        await configuration_parser.from_filesystem(Path('./configurations')) \
            .pipe(
                op.subscribe_on(scheduler),
                op.do(TimeRangeCalculator()),
                # TODO make single json file
                op.flat_map(monobank_api.from_configuration),
                # op.retry(3),
                op.filter(filters.TransferFilter()),
                *(op.do(o) for o in self.observers),
                op.do_action(on_error=lambda error: logging.error(f'{error.__class__.__name__}: {error}') or traceback.print_tb(error.__traceback__)),
            )

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    engine = RequestEngine()
    engine.add_group_observer(UploadObserver)
    
    logging.info('Creating loop')
    loop = asyncio.new_event_loop()
    loop.create_task(engine.run())
    aws = asyncio.all_tasks(loop)
    logging.info('Running loop tasks')
    while len(aws) > 0:
        loop.run_until_complete(asyncio.gather(*aws))
        aws = asyncio.all_tasks(loop)
