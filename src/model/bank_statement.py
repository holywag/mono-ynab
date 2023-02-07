from dataclasses import dataclass
from datetime import datetime
from model.configuration import Configuration, Account

@dataclass
class BankStatement:
    configuration: Configuration
    account: Account
    id: str
    time: datetime
    amount: float
    mcc: int
    payee: str
    description: str
