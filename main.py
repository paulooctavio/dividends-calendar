from b3_client import B3HttpClient
from calendar_api import CalendarAPI
from configs import get_configs
import pandas as pd


def clean_payments_df(df):
    df.drop_duplicates(inplace=True)
    df["Prev. Pagamento"] = (
        df["Prev. Pagamento"].str
        .replace('0001', '2021')
    )
    df["Prev. Pagamento"] = (
        pd.to_datetime(df["Prev. Pagamento"])
        .dt.date
    )
    return df


def get_payment_event(row):
    asset = row["Ativo"]
    symbol = row["Cód. Negociação"]
    date = row["Prev. Pagamento"]
    payment_type = row["Tipo Evento"]
    return {
        'summary': f'{asset} ({symbol}) - {payment_type}',
        'start': {
            'date': str(date)
        },
        'end': {
            'date': str(date)
        }
    }


def generate_calendar(row, calendar):
    event = get_payment_event(row)
    calendar.add_event(event)


def main(configs):
    client = B3HttpClient(
        username=configs["username"],
        password=configs["password"]
    )
    client.login()
    payments_df = client.get_payments()
    payments_df = clean_payments_df(payments_df)
    calendar = CalendarAPI()
    payments_df.apply(
        lambda row: generate_calendar(row, calendar),
        axis=1
    )


if __name__ == "__main__":
    configs = get_configs()
    main(configs)
