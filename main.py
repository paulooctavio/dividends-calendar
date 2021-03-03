from cei_client import CEIHttpClient
from calendar_api import CalendarAPI
from b3_client import B3HttPClient
from configs import get_configs


def get_payment_event(row, owned_assets):
    asset = row["company"]
    code = row["code"]
    ex_date = row["ex_date"]
    is_owned = True if code in owned_assets else False
    event = {
        'summary': f'{asset} ({code})',
        'colorId': '11' if is_owned else '9',
        'description': 'This asset is in your wallet' if is_owned else None,
        'start': {
            'date': str(ex_date)
        },
        'end': {
            'date': str(ex_date)
        }
    }
    print(event)
    return event


def generate_calendar(row, calendar, owned_assets):
    event = get_payment_event(row, owned_assets)
    calendar.add_event(event)


def main(configs):
    cei_http_client = CEIHttpClient(
        username=configs["username"],
        password=configs["password"]
    )
    cei_http_client.login()
    owned_assets = cei_http_client.get_owned_assets()

    b3_http_client = B3HttPClient()
    payments_df = b3_http_client.get_payments()

    calendar = CalendarAPI()
    payments_df.apply(
        lambda row: generate_calendar(row, calendar, owned_assets),
        axis=1
    )


if __name__ == "__main__":
    configs = get_configs()
    main(configs)
