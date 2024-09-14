import numpy as np
import pandas as pd

import requests
from datetime import date, timedelta

from pyxirr import xirr, DayCount
from typing import Union, Optional

def isin_secid(code: str, direction: Union[str, None] = "isin2secid") -> Optional[str]:
    """
    Looks up a security ID (SECID) or International Securities Identification Number (ISIN)
    on the Moscow Exchange ISS data feed.

    Args:
        code (str): The ISIN or SECID to look up.
        direction (Union[str, None], optional): The conversion direction. Must be
            either 'isin2secid' or 'secid2isin'. Defaults to "isin2secid".

    Returns:
        Optional[str]: The corresponding SECID or ISIN if found, otherwise None.
    """

    if direction not in ['isin2secid', 'secid2isin']:
        raise Exception("Direction should be either 'isin2secid' or 'secid2isin'")
    code = str(code).upper()

    try:
        data = requests.get(f'https://iss.moex.com/iss/securities.json?q={code}&iss.meta=off', timeout=5)
        data.raise_for_status()
        data = data.json()
    except requests.exceptions.RequestException as err:
        print(f'{err.__class__.__name__}: {err}')
        raise

    df = pd.DataFrame(data['securities']['data'], columns=data['securities']['columns'])

    if direction == 'isin2secid':
        try:
            res =  df.loc[df['isin'] == code, 'secid'].iat[0]
            return res
        except:
            print(f'No match for ISIN {code}')
    else:
        try:
            res = df.loc[df['secid'] == code, 'isin'].iat[0]
            return res
        except:
            print(f'No match for SECID {code}')


def moex_bond_info(ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches bond information from the Moscow Exchange ISS data feed for the given ticker.

    Args:
        ticker (str): The ticker symbol (SECID) of the bond to retrieve information for.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - A DataFrame containing general bond information (SECID, ISIN, name, etc.).
            - A DataFrame containing cash flow information (coupons, amortizations, offers).
    """

    try:
        res_nfo = requests.get(
            f'https://iss.moex.com/iss/securities/{ticker}.json', 
            timeout=5
        )
        res_nfo.raise_for_status()
        res_nfo = res_nfo.json()
    except requests.exceptions.RequestException as err:
        print(f'{err.__class__.__name__}: {err}')
        raise

    df_nfo = pd.DataFrame(res_nfo['description']['data'], columns=res_nfo['description']['metadata'])

    if df_nfo.empty:
        raise Exception(f"Didn't receive any data for SECID {ticker}")

    df_nfo = (
        df_nfo.rename(columns={'name': 'index', 'title': 'key'})
            .set_index('index')
            .drop(columns=['type', 'sort_order', 'is_hidden', 'precision'])
    )
    df_nfo = df_nfo[
        ~df_nfo.index.isin(['REGNUMBER', 'LATNAME', 'STARTDATEMOEX', 'PROGRAMREGISTRYNUMBER', 'COUPONDATE', 
                            'EVENINGSESSION', 'TYPENAME', 'GROUP', 'TYPE', 'GROUPNAME', 'EMITTER_ID']
    )]
    df_nfo.at['ISSUESIZE', 'value'] = f"{(float(df_nfo.at['ISSUESIZE', 'value']) * float(df_nfo.at['INITIALFACEVALUE', 'value']))/1e9:,.1f} млрд"

    try:
        res_yld = requests.get(
            f'https://iss.moex.com/iss/engines/stock/markets/bonds/securities/{ticker}.json?iss.only=securities', 
            timeout=5
        )
        res_yld.raise_for_status()
        res_yld = res_yld.json()
    except requests.exceptions.RequestException as err:
        print(f'{err.__class__.__name__}: {err}')
        raise
    df_yld = pd.DataFrame(res_yld['securities']['data'], columns=res_yld['securities']['columns'])
    df_yld = df_yld.loc[
            df_yld['BOARDID'].isin(['TQCB', 'TQOB']), 
            df_yld.columns.isin(['PREVWAPRICE', 'YIELDATPREVWAPRICE', 'ACCRUEDINT'])
        ].reset_index(drop=True).T
    df_yld.index.name = 'index'
    df_yld.insert(0, 'key', None)
    df_yld['key'] = ['Средневзвешенная цена пред. дня', 'Доходность по оценке пред. дня', 'НКД']
    df_yld = df_yld.rename(columns={0: 'value'})

    tdy = date.today().strftime('%Y-%m-%d')
    wk_ago = (date.today() + timedelta(days=-14)).strftime('%Y-%m-%d')
    try:
        res_vol = requests.get(
            f'https://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{ticker}.json?from={wk_ago}&till={tdy}&marketprice_board=1', 
            timeout=5
        )
        res_vol.raise_for_status()
        res_vol = res_vol.json()
    except requests.exceptions.RequestException as err:
        print(f'{err.__class__.__name__}: {err}')
        raise
    vol = pd.DataFrame(res_vol['history']['data'], columns=res_vol['history']['columns']).loc[:, 'VALUE'].mean()
    vol = 0 if np.isnan(vol) else vol
    df_vol = pd.DataFrame({'key': 'Среднедневной объем', 'value': f'{vol/1e6:,.1f} млн'}, index=['VOLUME'])

    df_nfo = pd.concat([df_nfo, df_yld, df_vol])
    df_nfo = df_nfo.reindex(['SECID', 'ISIN', 'NAME', 'SHORTNAME', 'LISTLEVEL', 'ISQUALIFIEDINVESTORS', 'ISSUESIZE', 
        'INITIALFACEVALUE', 'FACEUNIT', 'DAYSTOREDEMPTION', 'ISSUEDATE', 'MATDATE', 'BUYBACKDATE', 'FACEVALUE', 
        'COUPONPERCENT', 'COUPONVALUE', 'COUPONFREQUENCY', 'PREVWAPRICE', 'YIELDATPREVWAPRICE', 'ACCRUEDINT', 'VOLUME'], 
        fill_value='–'
    )
    df_nfo['key'] = df_nfo['key'].replace({
        'Бумаги для квалифицированных инвесторов': 'Для квал. инвесторов',
        'Первоначальная номинальная стоимость': 'Первоначальная номн. стоимость',
        'Дата к которой рассчитывается доходность': 'Дата для расчета доходности',
        'Сумма купона, в валюте номинала': 'Сумма купона',
        'Периодичность выплаты купона в год': 'Купонов в год'
    })
    df_nfo = df_nfo.fillna('–')

    try:
        res_cf = requests.get(
            f'https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/{ticker}.json?limit=100', 
            timeout=5
        )
        res_cf.raise_for_status()
        res_cf = res_cf.json()
    except requests.exceptions.RequestException as err:
        print(f'{err.__class__.__name__}: {err}')
        raise

    df_coupons = (
        pd.DataFrame(res_cf['coupons']['data'], columns=res_cf['coupons']['columns'])
        .reindex(columns=['coupondate', 'value'])
        .rename(columns={'coupondate': 'event_date', 'value': 'coupon'})
    )

    df_amt = (
        pd.DataFrame(res_cf['amortizations']['data'], columns=res_cf['amortizations']['columns'])
        .reindex(columns=['amortdate', 'value'])
        .rename(columns={'amortdate': 'event_date', 'value': 'amt'})
    )

    df_off = (
        pd.DataFrame(res_cf['offers']['data'], columns=res_cf['offers']['columns'])
        .reindex(columns=['offerdate', 'price', 'offertype'])
        .rename(columns={'offerdate': 'event_date', 'price': 'offer', 'offertype': 'offer_type'})
    )

    df_cf = df_coupons.merge(df_amt, how='outer', on='event_date')
    df_cf = df_cf.merge(df_off, how='outer', on='event_date')
    df_cf = df_cf.fillna('–')
    df_cf = df_cf.sort_values(by='event_date')
    df_cf.index = range(1, df_cf.shape[0] + 1) 
    df_cf.index.name = df_nfo.at['SHORTNAME', 'value']

    return df_nfo, df_cf


def moex_bond_yield(df_nfo: pd.DataFrame, df_cf: pd.DataFrame, price: float) -> float:
    """
    Calculates the Yield to Maturity (YTM) for a bond using the information in the provided DataFrames.

    Args:
        df_nfo (pd.DataFrame): DataFrame containing general bond information (SECID, ISIN, FACEVALUE, etc.).
        df_cf (pd.DataFrame): DataFrame containing cash flow information (coupons, amortizations, offers).
        price (float): The current market price of the bond.

    Returns:
        float: The calculated YTM as a percentage (rounded to two decimal places)
    """
    df_cf = df_cf.replace({'–': None})

    if df_cf['coupon'].isna().any() or not df_cf[['offer', 'offer_type']].isna().all().all():
        raise Exception("Can't calculate YTM for the bond")

    try:
        purchase_date = pd.to_datetime(date.today())
        facevalue = float(df_nfo.loc['FACEVALUE', 'value'])
        accruedint = float(df_nfo.loc['ACCRUEDINT', 'value'])
    except:
        print('Face value or accrued interest is missing')
        raise

    df_cf['event_date'] = pd.to_datetime(df_cf['event_date'])
    df_cf = df_cf[df_cf['event_date'] > purchase_date]
    
    dates = df_cf['event_date'].to_list()
    dates = [purchase_date] + dates
    amounts = df_cf[['coupon', 'amt']].sum(axis=1).to_list()
    amounts = [-price/100 * facevalue - accruedint] + amounts
    
    ytm = round(xirr(dates=dates, amounts=amounts, day_count=DayCount.ACT_ACT_ISDA)*100, 2)
    return ytm