from model.configuration import *
import os
from pathlib import Path

def from_filesystem(configs_dir: Path):
    return rx.from_iterable(os.listdir(configs_dir)) \
        .pipe(
            op.map(lambda p: configs_dir / p),
            op.filter(os.path.isdir),
            op.map(parse_config_from_json),
        )

def parse_config_from_json(config_dir: Path):
    return JsonConfiguration(
        json.load(open(config_dir / 'import_settings.json')),
        json.load(open(config_dir / 'accounts.json')),
        json.load(open(config_dir / 'categories.json')),
        json.load(open(config_dir / 'payees.json')))

class JsonConfiguration(Configuration):
    def __init__(self, import_settings_json, accounts_json, categories_json, payees_json):
        self.merge_transfer_statements = import_settings_json['merge_transfer_statements']
        self.remember_last_import_timestamp = import_settings_json['remember_last_import_timestamp']
        bank_settings = import_settings_json['bank'].copy()
        bank_settings['time_range'] = TimeRangeSettings(
            **{ k: datetime.fromisoformat(v) for k,v in bank_settings['time_range'].items()})
        self.bank = BankImportSettings(**bank_settings)
        self.ynab = YnabImportSettings(**import_settings_json['ynab'])
        self.accounts = [Account(**a) for a in accounts_json]
        self.statement_field_settings = StatementFieldSettings(
            accounts_by_transfer_payee_regex=
                RegexList([RegexItem(*a.transfer_payee, a) for a in self.accounts if len(a.transfer_payee)]),
            categories_by_payee_regex=
                RegexList([RegexItem(*c["criterias"].get("payee", []), YnabCategory(**c['ynab_category'])) 
                    for c in categories_json if len(c["criterias"].get("payee", []))]),
            payee_aliases_by_payee_regex=
                RegexList([RegexItem(*regexes, alias) for alias,regexes in payees_json.items() if len(regexes)]),
            categories_by_mcc=
                { mcc: YnabCategory(**c['ynab_category']) for c in categories_json for mcc in c['criterias'].get('mcc', []) })
