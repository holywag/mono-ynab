from model.configuration import *
import os

def from_filesystem(configs_dir):
    return rx.from_iterable(os.listdir(configs_dir)) \
        .pipe(
            op.map(lambda p: os.path.join(configs_dir, p)),
            op.filter(os.path.isdir),
            op.map(parse_config_from_json),
        )

def parse_config_from_json(config_dir):
    timestamp_path = os.path.join(config_dir, 'timestamp.json')
    return JsonConfiguration(
        json.load(open(os.path.join(config_dir, 'import_settings.json'))),
        json.load(open(os.path.join(config_dir, 'accounts.json'))),
        json.load(open(os.path.join(config_dir, 'categories.json'))),
        json.load(open(os.path.join(config_dir, 'payees.json'))),
        os.path.isfile(timestamp_path) and json.load(open(timestamp_path)))

class JsonConfiguration(Configuration):
    def __init__(self, import_settings_json, accounts_json, categories_json, payees_json, timestamp_json=None):
        self.merge_transfer_statements = import_settings_json['merge_transfer_statements']
        self.remember_last_import_timestamp = import_settings_json['remember_last_import_timestamp']
        self.bank = self.__bank_settings(
            import_settings_json['bank'], self.remember_last_import_timestamp and timestamp_json)
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

    @property
    def timestamp(self):
        return {'last_import': datetime.now(tz=self.bank.time_range.start.tzinfo).isoformat()}

    @staticmethod
    def __bank_settings(bank_settings_json, timestamp_json=None):
        last_import = timestamp_json and datetime.fromisoformat(timestamp_json['last_import'])
        time_range_json = bank_settings_json['time_range']
        time_range_start = datetime.fromisoformat(time_range_json['start'])
        if last_import and last_import > time_range_start:
            time_range_start = last_import
        bank_settings = bank_settings_json | { 'time_range' : TimeRangeSettings(
            start=time_range_start,
            end=time_range_json['end'] and datetime.fromisoformat(time_range_json['end']) or datetime.now(tz=time_range_start.tzinfo)) }
        return BankImportSettings(**bank_settings)
