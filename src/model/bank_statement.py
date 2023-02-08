from dataclasses import dataclass
from datetime import datetime
from model.configuration import Configuration, Account

@dataclass
class BankStatement:
    configuration: Configuration
    account: Account
    transfer_account: Account
    id: str
    time: datetime
    amount: int
    mcc: int
    description: str
