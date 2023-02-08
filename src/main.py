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

class UploadObserver(rx.typing.Observer):
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
        sleep_duration = random.randrange(2, 5)
        logging.info(f'UploadObserver: send_transactions sleep_duration={sleep_duration} n={len(self.transactions)} {self.transactions}')
        await asyncio.sleep(sleep_duration)
        logging.info(f'UploadObserver: send_transactions DONE')

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
        logging.debug(f'UploadObserver on_completed')
        asyncio.create_task(self.send_transactions())

    def on_error(self, error: Exception):
        logging.error(f'UploadObserver: on_error({error.__class__.__name__}: {error})')
        traceback.print_tb(error.__traceback__)

class RequestEngine:
    def __init__(self):
        self.observers = []

    def add_transaction_observer(self, observer, group_by_configuration=False):
        """Add an observer to the chain of transactions.

        If group_by_configuration is True, the chain of transactions from all configurations
            is groupped by configuration and the observer is created and subscribed to each 
            transaction chain separately.
        If group_by_configuration is False, the observer is created once and subscribed to
            a single chain of transactions fetched from all configurations.
        """
        if group_by_configuration:
            self.observers.append(observer)
        else:
            observer = observer()
            self.observers.append(lambda: observer)

    async def run(self):
        configuration_parser.from_filesystem(Path('./configurations')) \
            .pipe(
                # TODO: calculate new time range, implement overriding
                op.flat_map(monobank_api.from_configuration),
                # op.retry(3),
                # TODO: adapt tranfer filter
                # op.filter(filters.TransferFilter()),
                *(op.do(o()) for o in self.observers),
            ) \
            .subscribe(
                # TODO: store the updated time range configuration
                on_error=lambda error: logging.error(f'{error.__class__.__name__}: {error}') or traceback.print_tb(error.__traceback__),
                scheduler=AsyncIOScheduler(asyncio.get_running_loop())
            )

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    engine = RequestEngine()
    engine.add_transaction_observer(UploadObserver, group_by_configuration=True)
    
    logging.info('Creating loop')
    loop = asyncio.new_event_loop()
    loop.create_task(engine.run())
    aws = asyncio.all_tasks(loop)
    logging.info('Running loop tasks')
    while len(aws) > 0:
        loop.run_until_complete(asyncio.gather(*aws))
        aws = asyncio.all_tasks(loop)
