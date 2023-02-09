from model.bank_statement import BankStatement

class TransferFilter:
    """Filter object that prevents duplication of transfers between two accounts.
    The transfer operation is usually represented by two transactions, importing 
    both of them will double the transfer amount.
    This filter object will filter out the 2nd transaction in a pair of transactions
    that describe the same transfer operation.
    """

    def __init__(self):
        self.transfer_statements = []

    def __make_transfer_id(statement: BankStatement, forward: bool):
        src = statement.account.iban
        dst = statement.transfer_account.iban
        amount = statement.amount
        return forward and f'{src}_{dst}_{amount}' or f'{dst}_{src}_{-amount}'

    def __call__(self, statement: BankStatement):
        if statement.transfer_account:
            try:
                self.transfer_statements.remove(TransferFilter.__make_transfer_id(statement, False))
                return False
            except ValueError:
                self.transfer_statements.append(TransferFilter.__make_transfer_id(statement, True))
        return True
