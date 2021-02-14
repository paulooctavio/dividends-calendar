import requests
import ssl
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from dataclasses import dataclass
from werkzeug.utils import cached_property

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
ssl._create_default_https_context = ssl._create_unverified_context


@dataclass
class Broker():
    value: str
    date: str


class B3HttpClient():
    IS_LOGGED = False
    SESSION = None
    LOGIN_URL = 'https://ceiapp.b3.com.br/CEI_Responsivo/./login.aspx'
    PAYMENTS_URL = (
        'https://ceiapp.b3.com.br/CEI_Responsivo/ConsultarProventos.aspx'
    )
    session = requests.Session()

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def login(self):
        response = self.session.get(
            self.LOGIN_URL,
            verify=False
        )
        login_page_content = response.content
        login_page_parsed = BeautifulSoup(
            login_page_content,
            "html.parser"
        )
        view_state = login_page_parsed.find(id='__VIEWSTATE')['value']
        viewstate_generator = login_page_parsed.find(
            id='__VIEWSTATEGENERATOR'
        )['value']
        event_validation = login_page_parsed.find(
            id='__EVENTVALIDATION'
        )['value']

        payload = {
                    'ctl00$ContentPlaceHolder1$smLoad': (
                        'ctl00$ContentPlaceHolder1$UpdatePanel1|'
                        'ctl00$ContentPlaceHolder1$btnLogar'
                    ),
                    '__EVENTTARGET': '',
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATEGENERATOR': viewstate_generator,
                    '__EVENTVALIDATION': event_validation,
                    '__VIEWSTATE': view_state,
                    'ctl00$ContentPlaceHolder1$txtLogin': self.username,
                    'ctl00$ContentPlaceHolder1$txtSenha': self.password,
                    '__ASYNCPOST': True,
                    'g-recaptcha-response': None,
                    'ctl00$ContentPlaceHolder1$btnLogar': 'Entrar'
                }

        headers = {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Referer': 'https://cei.b3.com.br/CEI_Responsivo/login.aspx',
            'Origin': 'https://cei.b3.com.br',
            'Host': 'cei.b3.com.br',
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 '
                'Safari/537.36'
            ),
        }

        response = self.session.post(
            self.LOGIN_URL,
            data=payload,
            headers=headers
        )
        self.session.headers.update(headers)
        if response.status_code == 200:
            self.IS_LOGGED = True

    @cached_property
    def payments_page(self):
        if not self.IS_LOGGED:
            self.login()
        response = self.session.get(
                self.PAYMENTS_URL,
                verify=False
            )
        return response.content

    def get_brokers(self):
        payments_page = self.payments_page
        id = 'ctl00_ContentPlaceHolder1_ddlAgentes'
        soup = BeautifulSoup(payments_page, 'html.parser')
        brokers_select = soup.find('select', id=id)
        brokers_option = brokers_select.find_all('option')
        date = soup.find(
            id='ctl00_ContentPlaceHolder1_txtData'
        )['value']

        return [
            Broker(
                value=broker_option['value'],
                date=date
            )
            for broker_option in brokers_option
            if broker_option['value'] != -1
        ]

    def get_payments(self):
        brokers = self.get_brokers()
        payments_page = self.payments_page
        for broker in brokers:
            payments_page_parsed = BeautifulSoup(
                payments_page,
                "html.parser"
            )
            view_state = payments_page_parsed.find(id='__VIEWSTATE')['value']
            viewstate_generator = payments_page_parsed.find(
                id='__VIEWSTATEGENERATOR'
            )['value']
            event_validation = payments_page_parsed.find(
                id='__EVENTVALIDATION'
            )['value']

            payload = {
                        'ctl00$ContentPlaceHolder1$ToolkitScriptManager1:': (
                            'ctl00$ContentPlaceHolder1$updFiltro|'
                            'ctl00$ContentPlaceHolder1$btnConsultar'
                        ),
                        (
                            'ctl00_ContentPlaceHolder1_'
                            'ToolkitScriptManager1_HiddenField'
                        ): '',
                        '__EVENTTARGET': '',
                        '__EVENTARGUMENT': '',
                        '__VIEWSTATE': view_state,
                        '__VIEWSTATEGENERATOR': viewstate_generator,
                        '__EVENTVALIDATION': event_validation,
                        'ctl00$ContentPlaceHolder1$ddlAgentes': broker.value,
                        'ctl00$ContentPlaceHolder1$ddlContas': 0,
                        'ctl00$ContentPlaceHolder1$txtData': broker.date,
                        '__ASYNCPOST': True,
                        'ctl00$ContentPlaceHolder1$btnConsultar': 'Consultar'
                    }

            consult_response = self.session.post(
                self.PAYMENTS_URL,
                data=payload,
                verify=False
            )
            consult_page = consult_response.content
            consult_page_parsed = BeautifulSoup(
                consult_page,
                'lxml'
            )
            div = consult_page_parsed.find(
                id='ctl00_ContentPlaceHolder1_updFiltro'
            )
            payments_table = div.find_all("table")[0]
            payments_df = pd.read_html(str(payments_table))[0]
            payments_df.drop(
                payments_df.tail(1).index,
                inplace=True
            )
            payments_df = payments_df[
                [
                    'Ativo',
                    'Cód. Negociação',
                    'Prev. Pagamento',
                    'Tipo Evento'
                ]
            ]
            return payments_df
