import re, typing
from datetime import datetime
from dataclasses import dataclass

@dataclass
class Configuration:
    remove_cancelled_statements: bool
    merge_transfer_statements: bool
    remember_last_import_timestamp: bool
    bank: BankImportSettings
    ynab: YnabImportSettings
    accounts: list[Account]
    statement_field_settings: StatementFieldSettings

@dataclass
class BankImportSettings:
    token: str
    n_retries: int
    time_range: TimeRangeSettings

@dataclass
class TimeRangeSettings:
    start: datetime
    end: datetime

@dataclass
class YnabImportSettings:
    token: str
    budget_name: str

@dataclass
class Account:
    enabled: bool
    ynab_name: str
    iban: str
    transfer_payee: list[str]

@dataclass
class StatementFieldSettings:
    accounts_by_transfer_payee_regex: RegexList[RegexItem]
    categories_by_mcc: dict[Mcc, YnabCategory]
    categories_by_payee_regex: RegexList[RegexItem]
    payee_aliases_by_payee_regex: RegexList[RegexItem]

Mcc = int

class RegexList(list):
    def get(self, key, default=None, condition=lambda _: True) -> str:
        return next((r.value for r in self if r.regex_key.match(key) and condition(r.value)), default)

@dataclass
class RegexItem:
    regex_key: re.Pattern
    value: typing.Any

    def __init__(self, patterns: list[str], value: typing.Any):
        self.regex_key=re.compile(f'(?:{"|".join(patterns)})')
        self.value=value
