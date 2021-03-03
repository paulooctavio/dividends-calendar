import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date

class B3HttPClient():
    LISTED_COMPANIES_URL = (
        'http://bvmf.bmfbovespa.com.br/cias-listadas/'
        'empresas-listadas/BuscaEmpresaListada.aspx?idioma=en-us'
    )
    CORPORATE_ACTIONS_URL = (
        'http://bvmf.bmfbovespa.com.br'
        '/cias-listadas/empresas-listadas/'
        'ResumoEventosCorporativos.aspx?codigoCvm={}&tab=3&idioma=en-us'
    )

    def get_raw_payments_df(self):
        payload = {
                    'ctl00$ContentPlaceHolder1$smLoad': (
                        'ctl00$ContentPlaceHolder1$UpdatePanel1|'
                        'ctl00$ContentPlaceHolder1$btnLogar'
                    ),
                    '__EVENTTARGET': (
                        'ctl00:contentPlaceHolderConteudo:'
                        'BuscaNomeEmpresa1:btnTodas'
                    ),
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATEGENERATOR': '9CFBB4E5',
                    '__EVENTVALIDATION': (
                        '/wEWKQLd1JKADQKrqYeWCwL85ODVCQLh+8LADwKyt5yADgK'
                        'Xzv7qAwLoidiqAgLNoLqVCALuyLaBBALT35jsCQKkm/KrCA'
                        'KJstSWDgLa7a3WDAK/hJDBAgKQwOmAAQL11svrBgL2pYL/B'
                        'wLbvOTpDQKs+L2pDAKRj6CUAgLiyvlTAsfh274GApidtf4E'
                        'Av2zl+kKAp7ck9UGAoPz9b8MAtSuz/8KApb/x9cCAvuVqsI'
                        'IAszRg4IHArHo5ewMAoKkv6wLAue6oZcBArj2+tYPAp2N3c'
                        'EFAr612a0BAqPMu5gHAqqynsAFAvrzvPwMAr/S5NkCArSz5'
                        'PIOftbcjad1FZzl+NIPhyp/vfMM4co='
                    ),
                    '__VIEWSTATE': (
                        '/wEPDwULLTE2OTg3NzU5NjgPZBYCZg9kFgQCAQ9kFgICEA8'
                        'WAh4EVGV4dAUHbWFya2V0c2QCAw9kFgICAQ9kFgQCAQ9kFg'
                        'ICAQ9kFgYCBQ8PFgIfAAUKMDIvMTcvMjAyMWRkAgkPDxYCH'
                        'wAFCDExaDAwIFBNZGQCEQ8PFgIeBUNvdW50AgRkFgICAw8P'
                        'FgYeAklEBRpwdndCdXNjYUFub0xpc3RhZ2VtRW1wcmVzYR4'
                        'HVG9vbFRpcGUeB1Zpc2libGVoZBYCAgEPZBYCAgEPEGRkFg'
                        'BkAgMPD2QWAh4Hb25jbGljawUcaGlzdG9yeS5iYWNrKCk7I'
                        'HJldHVybiBmYWxzZWQYBgU8Y3RsMDAkY29udGVudFBsYWNl'
                        'SG9sZGVyQ29udGV1ZG8kQnVzY2FTZXRvckVtcHJlc2ExJGd'
                        'yZFNldG9yDxQrAAJkFwBkBUFjdGwwMCRjb250ZW50UGxhY2'
                        'VIb2xkZXJDb250ZXVkbyRCdXNjYVNlZ21lbnRvRW1wcmVzY'
                        'TEkZ3JkRW1wcmVzYQ8UKwACZBcAZAUeX19Db250cm9sc1Jl'
                        'cXVpcmVQb3N0QmFja0tleV9fFggFPWN0bDAwJGNvbnRlbnR'
                        'QbGFjZUhvbGRlckNvbnRldWRvJEJ1c2NhTm9tZUVtcHJlc2'
                        'ExJGdyZEVtcHJlc2EFPGN0bDAwJGNvbnRlbnRQbGFjZUhvb'
                        'GRlckNvbnRldWRvJEJ1c2NhU2V0b3JFbXByZXNhMSRncmRT'
                        'ZXRvcgU+Y3RsMDAkY29udGVudFBsYWNlSG9sZGVyQ29udGV'
                        '1ZG8kQnVzY2FTZXRvckVtcHJlc2ExJGdyZEVtcHJlc2EFQW'
                        'N0bDAwJGNvbnRlbnRQbGFjZUhvbGRlckNvbnRldWRvJEJ1c'
                        '2NhU2VnbWVudG9FbXByZXNhMSRncmRFbXByZXNhBURjdGww'
                        'MCRjb250ZW50UGxhY2VIb2xkZXJDb250ZXVkbyRCdXNjYUF'
                        'ub0xpc3RhZ2VtRW1wcmVzYTEkZ3JkRW1wcmVzYQUvY3RsMD'
                        'AkY29udGVudFBsYWNlSG9sZGVyQ29udGV1ZG8kQWpheFBhb'
                        'mVsQnVzY2EFK2N0bDAwJGNvbnRlbnRQbGFjZUhvbGRlckNv'
                        'bnRldWRvJG1wZ1BhZ2luYXMFNmN0bDAwJGNvbnRlbnRQbGF'
                        'jZUhvbGRlckNvbnRldWRvJHRhYk1lbnVFbXByZXNhTGlzdG'
                        'FkYQVEY3RsMDAkY29udGVudFBsYWNlSG9sZGVyQ29udGV1Z'
                        'G8kQnVzY2FBbm9MaXN0YWdlbUVtcHJlc2ExJGdyZEVtcHJl'
                        'c2EPFCsAAmQXAGQFPmN0bDAwJGNvbnRlbnRQbGFjZUhvbGR'
                        'lckNvbnRldWRvJEJ1c2NhU2V0b3JFbXByZXNhMSRncmRFbX'
                        'ByZXNhDxQrAAJkFwBkBT1jdGwwMCRjb250ZW50UGxhY2VIb'
                        '2xkZXJDb250ZXVkbyRCdXNjYU5vbWVFbXByZXNhMSRncmRF'
                        'bXByZXNhDxQrAAJkFwBktCLNPzLBW6i+C2s3xkzpJIXL+T0='
                    ),
                    'ctl00_contentPlaceHolderConteudo_AjaxPanelBuscaPostDataValue': (
                        'ctl00_contentPlaceHolderConteudo_AjaxPanelBusca,'
                        'ActiveElement,'
                        'ctl00_contentPlaceHolderConteudo_BuscaNomeEmpresa1_btnTodas;'),
                    'ctl00$contentPlaceHolderConteudo$tabMenuEmpresaListada': {
                        "State": {},
                        "TabState": {
                            (
                                'ctl00_contentPlaceHolderConteudo_tabMenu'
                                'EmpresaListada_tabNome'
                            ): {
                                "Selected": True
                            }
                        }
                    },
                    (
                        'ctl00$contentPlaceHolderConteudo$BuscaNomeEmpresa1$'
                        'txtNomeEmpresa$txtNomeEmpresa'
                    ): '',
                    'ctl00$contentPlaceHolderConteudo$mpgPaginas_Selected': 0,
                    'RadAJAXControlID': (
                        'ctl00_contentPlaceHolderConteudo'
                        '_AjaxPanelBusca'
                    ),
                    'httprequest': True
                }

        headers = {
            'content-type': 'application/x-www-form-urlencoded',
            'Referer': 'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/BuscaEmpresaListada.aspx?idioma=en-us',
            'Origin': 'http://bvmf.bmfbovespa.com.br',
            'Host': 'bvmf.bmfbovespa.com.br',
            'User-Agent': (
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 '
                'Safari/537.36'
            ),
        }

        response = requests.post(
            self.LISTED_COMPANIES_URL,
            data=payload,
            headers=headers
        )

        page_parsed = BeautifulSoup(
            response.text,
            "html.parser"
        )

        rows = page_parsed.find(
            'table',
            {
                'class': 'MasterTable_SiteBmfBovespa'
            }
        ).find_all('a')

        payments = pd.DataFrame()
        for i, row in enumerate(rows):
            if i % 2 == 0:
                company = row.text
                cvm_code = row['href'].split('=')[1]
                response = requests.get(
                    self.CORPORATE_ACTIONS_URL.format(cvm_code),
                    headers=headers
                )
                page_parsed = BeautifulSoup(
                    response.text,
                    "html.parser"
                )
                payments_id = (
                    'ctl00_contentPlaceHolderConteudo_'
                    'grdDividendo_ctl01'
                )
                if page_parsed.find(id=payments_id):
                    company_payments_table = page_parsed.find(
                        id=payments_id
                    )
                    company_payments_df = pd.read_html(
                        str(company_payments_table)
                    )[0]
                    code_id = (
                        'ctl00_contentPlaceHolderConteudo_'
                        'lblCodigoValor'
                    )
                    company_code = page_parsed.find(id=code_id).text
                    company_payments_df['code'] = company_code
                    company_payments_df['company'] = company
                    payments = pd.concat([payments, company_payments_df])
        return payments

    def get_payments(self):
        payments = self.get_raw_payments_df()
        payments.rename(
            columns={
                'Cash Dividends': 'payment_type',
                'Last Date Prior to "EX"': 'ex_date',
                'Rate (R$)': 'rate',
                'Payment Date': 'payment_date'
            },
            inplace=True
        )
        payments = payments[
            payments['payment_type'] == 'DIVIDENDO'
        ]
        date_columns = ['payment_date', 'ex_date']
        for date_column in date_columns:
            payments[date_column] = payments[date_column].replace(
                '12/31/9999',
                ''
            )
            payments[date_column] = pd.to_datetime(
                payments[date_column]
            ).dt.date
        payments = payments[
            payments['payment_date'].astype('datetime64').dt.year
            >= date.today().year
        ]
        payments = payments[
            [
                'payment_type', 'ex_date', 'rate',
                'payment_date', 'company', 'code'
            ]
        ]
        payments.drop_duplicates(inplace=True)
        return payments
