from model.configuration import Configuration
from model.bank_statement import BankStatement
import rx
from rx import operators as op
import monobank
from datetime import datetime

def from_configuration(configuration: Configuration) -> rx.typing.Observable[BankStatement]:
    # TODO: implement real logic of request
    # mono_api = monobank.MonobankApi(monobank.ApiClient(token))
    return rx.from_iterable([
        {'id': 'mcZtdMkl9ghAMxEH', 'time': 1674199375, 'description': 'zakaz.ua', 'mcc': 5411, 'originalMcc': 5411, 'amount': -130906, 'operationAmount': -130906, 'currencyCode': 980, 'commissionRate': 0, 'cashbackAmount': 1309, 'balance': 1600758, 'hold': False, 'receiptId': '2CCA-XE7K-76E5-B47P'},
        {'id': 'ZkaUW3gHwhNufTly', 'time': 1675368278, 'description': 'Переказ на картку', 'mcc': 4829, 'originalMcc': 4829, 'amount': -3291869, 'operationAmount': -3291869, 'currencyCode': 980, 'commissionRate': 0, 'cashbackAmount': 0, 'balance': 2406295, 'hold': True}
    ]) \
    .pipe(
        op.map(parse_statement(configuration)),
    )

def parse_statement(configuration: Configuration):
    """Convert data received from Monobank API to BankStatement object
    for further processing.
    """
    def __do_parse(s: dict) -> BankStatement:
        return BankStatement(
            configuration=configuration,
            account=configuration.accounts[0],
            id=s['id'],
            time=datetime.fromtimestamp(int(s['time'])),
            amount=s['amount'],
            mcc=int(s['mcc']),
            payee=s['description'],
            description=s['description'])
    return __do_parse
    