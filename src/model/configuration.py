import re
from typing import Any
from datetime import datetime
from dataclasses import dataclass

@dataclass
class TimeRangeSettings:
    start: datetime
    end: datetime

@dataclass
class BankImportSettings:
    token: str
    n_retries: int
    time_range: TimeRangeSettings

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

class RegexList(list):
    def get(self, key: str, default: Any = None, condition=lambda _: True) -> str:
        return next((r.value for r in self if r.regex_key.match(key) and condition(r.value)), default)

@dataclass
class RegexItem:
    regex_key: re.Pattern
    value: Any

    def __init__(self, patterns: list[str], value: Any):
        self.regex_key=re.compile(f'(?:{"|".join(patterns)})')
        self.value=value

Mcc = int

@dataclass
class YnabCategory:
    group: str
    name: str

@dataclass
class StatementFieldSettings:
    accounts_by_transfer_payee_regex: RegexList[RegexItem]
    categories_by_mcc: dict[Mcc, YnabCategory]
    categories_by_payee_regex: RegexList[RegexItem]
    payee_aliases_by_payee_regex: RegexList[RegexItem]

@dataclass
class Configuration:
    merge_transfer_statements: bool
    bank: BankImportSettings
    ynab: YnabImportSettings
    accounts: list[Account]
    statement_field_settings: StatementFieldSettings
