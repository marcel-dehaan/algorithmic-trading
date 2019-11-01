from functions.py import *
import pandas as pd
import numpy as np

from google.cloud import bigquery
from google.cloud import firestore
from google.cloud.exceptions import NotFound

import pyarrow
from datetime import datetime, timedelta, time
from calendar import monthrange
import timeit
import time as t

import os
from ib_insync import *
import pytz

while read_tickers(document=DOCUMENT):

    tickers_in_doing = read_tickers(document='doing',
                                    user=USER,
                                    account=ACCOUNT)
    if tickers_in_doing:
        ticker = tickers_in_doing[0]

        print(f'\n{"#" * 52}\n{"#" * 15} DOING LIST NOT EMPTY {"#" * 15}\n{"#" * 52}\n')

        print(f'{len(tickers_in_doing)} ticker(s) in doing')


    else:
        tickers = read_tickers(document=DOCUMENT)
        print(f'{len(tickers)} ticker in todo')
        ticker = tickers[0]

        print(f'Moving {ticker} from {DOCUMENT} to doing')
        ticker_array_action(document=DOCUMENT,
                            ticker=ticker,
                            action='DEL')

        # add ticker to doing
        ticker_array_action(document='doing',
                            ticker=ticker,
                            action='ADD',
                            user=USER,
                            account=ACCOUNT)

    contract = Stock(ticker, 'SMART', 'USD')
    contract_details = IB.reqContractDetails(contract)
    IB.qualifyContracts(contract)
    if len(contract_details) != 1:
        print(f'{len(contract_details)} contracts found. Moving {ticker} from doing to bad_contract  \n\n{"=" * 52}\n')
        ticker_array_action(document='doing',
                            ticker=ticker,
                            action='DEL',
                            user=USER,
                            account=ACCOUNT)

        ticker_array_action(document='bad_contract',
                            ticker=ticker,
                            action='ADD')
        continue

    for tick_type, postfix in tick_dict.items():

        table = dataset.table(ticker + '_' + postfix)

        print(f'\n{"=" * 52}\n\n{ticker}')
        print(f'Tick type: {tick_type}')
        print(f'BigQuery Table: {table.table_id}')
        table_exists = check_table_existence(table)

        if table_exists:

            sql = f""" 
            SELECT
              MAX(datetime)
            FROM
              {dataset.dataset_id}.{table.table_id}
            """
            df = BQ.query(sql).to_dataframe()
            start = df.iloc[0] + timedelta(seconds=1)
            start = start.to_pydatetime()
            print(f'Table exists \nstart: {start}')

        else:
            print(f'Table does not exists \n\nSearching for first date...')
            start = utc(first_date_with_tick_data(contract, tick_type))
        print(f'\n{"-" * 52}\n')

        end = ''
        tick_list = []
        total_tick_count = 0
        print('Start data retrieval')

        while True:

            if start > utc(datetime.now()):

                if tick_list:
                    add_tick_order(tick_list)
                    write_ticks_to_bigquery(tick_type, tick_list, bigquery, BQ, table)

                print(
                    f'{start.strftime("%Y-%m-%d %H:%M:%S")} in future.\n{len(tick_list)} ticks written to BigQuery in last load')
                if postfix == 'T':
                    print(f'Moving {ticker} from doing to maintain')
                    ticker_array_action(document='doing',
                                        ticker=ticker,
                                        action='DEL',
                                        user=USER,
                                        account=ACCOUNT)
                    ticker_array_action(document='maintain',
                                        ticker=ticker,
                                        action='ADD')

                break

            IB = check_for_restart(utc(time(RESTART_HOUR, RESTART_MINUTE)), IB, list(account_map['A'].values())[0], 0)
            ticks = IB.reqHistoricalTicks(contract, start, end, 1000, tick_type, useRth=False)
            if not ticks:
                start += timedelta(hours=4)
                continue

            elif ticks[0].time != ticks[-1].time:
                del ticks[0]
                tick_list = add_ticks(tick_type, ticks, tick_list)
                printProgress(ticks, tick_list)
                start = ticks[-1].time + timedelta(seconds=1)
                total_tick_count += len(ticks)

                if len(tick_list) > 20000:
                    tick_list = add_tick_order(tick_list)
                    write_ticks_to_bigquery(tick_type, tick_list, bigquery, BQ, table)
                    tick_list.clear()

            else:
                start += timedelta(hours=4)
                print(start)