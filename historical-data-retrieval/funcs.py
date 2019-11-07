from ib_insync import util
import os
from datetime import datetime, timedelta, time
from google.cloud import bigquery, storage, firestore
from ib_insync import *


def connect_ib(port, account, ip_address='127.0.0.1', client_id_range=11, IB=IB):
    """ Creates IB Client

    Args:
        port: port to connect to.
        ip_address: ip address
        client_id_range: the range of clients to try. 11 -> 0 to 10
        IB:

    Returns:
        BQ: Interactive Broker Client

    Raises:
        Exception: If no client_id are available on the port.
    """

    util.startLoop()
    ib = IB()

    for client_id in range(0, client_id_range):
        try:
            IB = ib.connect(ip_address, port, client_id)
            print(f'Connected to IB. Account: {account}. Port: {port}. Client_id: {client_id}')
            return IB
        except:
            continue

    raise Exception(f'no clients available, check settings')


def connect_bigquery():
    """Connects BigQuery Client.

    Returns:
        BigQuery Client

    ### TODO: Error handling
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'bq_service_account.json'
    BQ = bigquery.Client()
    print(f'BigQuery Project: {BQ.project}')
    return BQ


def connect_firestore():
    """Connects FireStore Client.

    Returns:
        Firestore Client

    ### TODO: Error handling
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'fs_service_account.json'
    FS = firestore.Client()
    print(f'Firestore Project: {FS.project}')
    return FS


def read_tickers(document: str, data_type='tick', user=None, account=None):
    """Get ticker list in firestore document

    Args:
        document: firestore document
        data_type: data type/resolution
        user: user
        account: IB account

    Returns:
        ticker list

    Raises:
        AssertionError
    """

    assert document in ['todo', 'doing', 'maintain', 'bad_contract']
    assert data_type in ['tick', '1s,' '30s']
    assert user in [None, 'jules', 'marcel']
    assert account in [None, 'A', 'B']

    doc_ref = FS.collection(u'universes').document(f'{document}')

    if document is 'doing':
        doc = doc_ref.get()
        doc_dict = doc.to_dict()
        return doc_dict[user][account][data_type]


    else:
        doc = doc_ref.get()
        doc_dict = doc.to_dict()
        return doc_dict[data_type]


def ticker_array_action(document: str, ticker: str, action: str, data_type='tick', user=None, account=None):
    """Add or remove ticker from ticker list in firestore document

    Args:
        document: firestore document
        ticker: ticker
        action: remove or delete
        data_type: data type/resolution
        user: user
        account: IB account

    Returns:
        ticker list

    Raises:
        AssertionError
    """

    assert document in ['todo', 'doing', 'maintain', 'bad_contract']
    assert data_type in ['tick', '1s,' '30s']
    assert user in [None, 'jules', 'marcel']
    assert action in ['ADD', 'DEL']
    assert account in [None, 'A', 'B']

    doc_ref = FS.collection(u'universes').document(f'{document}')
    array_action = firestore.ArrayUnion([ticker]) if action is 'ADD' else firestore.ArrayRemove([ticker])

    if document is 'doing':
        doc_ref.update({
            f'{user}': {
                f'{account}': {
                    f'{data_type}': array_action
                }
            }
        })

    else:
        doc_ref.update({
            f'{data_type}': array_action
        })


def create_dataset(BQ, dataset_name: str):
    """Creates a new BigQuery Dataset

    Args:
        BQ: A connected BigQuery Client
        dataset_name: The name of the Dataset

    Returns:
        Dataset reference

    ### TODO: Error handling
    """

    BQ.create_dataset(BQ.dataset(dataset_name))
    return BQ.dataset(dataset_name)


def check_table_existence(table_ref):
    """Checks if a BigQuery table exists.

    Args:
        table: table reference. E.g. dataset.table('table_name')
    Returns:
        Boolean. True if Table exists. False if table does not exist.
    Raises:
        Raises all exceptions (except NotFound errors).
    """

    try:
        BQ.get_table(table_ref)
        return True
    except NotFound:
        return False
    except Exception as e:
        print(f'Unexpected error: {e.message}')


def fetch_latest_table_dt(BQ, dataset_name: str, table_name: str):
    """ Return the most recent datetime in the table

    Args:
        BQ: BigQuery Client
        dataset_name: name of target dataset
        table_name: name of target table
    Returns:
        datetime.datetime object
    """

    sql = f""" 
        SELECT
          MAX(datetime)
        FROM
          {dataset_name}.{table_name}
        """
    df = BQ.query(sql).to_dataframe()
    return df.iloc[0]


def first_date_with_tick_data(contract, tick_type):
    """Searches for the first day historical tick data is available for a security.

    Args:
        contract: IB security contract
        tick_type: BID_ASK or TRADES

    Returns:
        datetime
    """

    def fetchTicks(contract, start):
        end = ''
        print(start.strftime("%Y-%m-%d"))
        return IB.reqHistoricalTicks(contract, start, end, 10, tick_type, useRth=False)

    current_year = datetime.now().year
    years = list(range(current_year - 4, current_year + 1))

    # determine the year data is first available
    for year in years:
        month = 12
        day = 31
        if year == current_year:
            month = datetime.now().month - 1
            day = monthrange(year, month)[1]
        if fetchTicks(contract, datetime(year, month, day)):
            break

    # determine the month data is first available
    days_of_month = {}
    months = list(range(1, month + 1))
    for month in months:
        days_of_month[month] = list(range(1, monthrange(year, month)[1] + 1))

    f, s = split_list(months)
    for months in [f, s]:
        month = months[-1]
        day = days_of_month[month][-1]
        if fetchTicks(contract, datetime(year, month, day)):
            break

    for month in months:
        day = days_of_month[month][-1]
        if fetchTicks(contract, datetime(year, month, day)):
            break

    # determine the day data is first available
    f, s = split_list(days_of_month[month])  # split month in half
    for days in [f, s]:
        if fetchTicks(contract, datetime(year, month, days[-1])):
            break

    f, s = split_list(days)  # split half month in half
    for days in [f, s]:
        if fetchTicks(contract, datetime(year, month, days[-1])):
            break
        else:
            days = s
            break

    for day in days:
        # walk through remaining days
        if fetchTicks(contract, datetime(year, month, day)):
            dt = datetime.now() - datetime(year, month, day)
            print(f'{dt.days} days data available')
            return datetime(year, month, day)


def check_for_restart(r_time, IB, port, BQ):
    """Check if present time is within two minutes preceding the IB Gateway
    restart. If so, disconnect and reconnect after restart.

    Args:
        r_time: restart time. Should be in UTC'
        IB: IB Client.
        port: ib port
        BQ: BigQuery Client

    Returns:
        (re)connected IB Client
    """

    now = utc(datetime.now())
    lower_bound_td = timedelta(seconds=-120)
    upper_bound_td = timedelta(seconds=+30)
    lower_bound = now.replace(hour=r_time.hour, minute=r_time.minute, second=0, microsecond=0) + lower_bound_td
    upper_bound = now.replace(hour=r_time.hour, minute=r_time.minute, second=0, microsecond=0) + upper_bound_td

    if now > lower_bound and now < upper_bound:
        print(f'\n{"#" * 52}\n{"#" * 8} GOING TO SLEEP IN AWAIT OF RESTART {"#" * 8}\n{"#" * 52}\n')
        IB.disconnect()
        util.sleep(420)
        IB = connect_ib(port, BQ)
    return IB


def tick_type_value_error(tick_type):
    return f'tick_type: {tick_type} not recognised. Should be \'BID_ASK\' or \'TRADES\'.'


def add_ticks(tick_type: str, ticks: list, tick_list: list):
    """ Adds ticks to the tick list.

    Args:
        tick_type: Has to be 'BID_ASK' or 'TRADES'
        ticks: The ticks to be appended to the tick_list
        tick_list: The list which aggregates ticks to write to a BigQuery Table
    Raises:
        ValueError: tick_type not recognised

    Returns:
        tick_list: The tick list with the added ticks.
    """

    if tick_type == 'BID_ASK':
        tick_list += [
            [tick.time, tick.priceBid, tick.sizeBid, tick.priceAsk, tick.sizeAsk, tick.tickAttribBidAsk.bidPastLow,
             tick.tickAttribBidAsk.askPastHigh] for tick in ticks]

    elif tick_type == 'TRADES':
        tick_list += [[tick.time, tick.price, tick.size, tick.exchange, tick.specialConditions.replace(' ', '')] for
                      tick in ticks]

    else:
        raise ValueError(tick_type_value_error(tick_type))
    return tick_list


def add_tick_order(tick_list):
    """ Add order variable to record sub-second order of ticks (0: first tick in second). Create dataframe """
    prev_dt = None
    count = 0

    for tick in tick_list:
        dt = tick[0]
        if dt == prev_dt:
            count += 1
            tick.insert(1, count)
            prev_dt = tick[0]
        else:
            count = 0
            tick.insert(1, count)
            prev_dt = tick[0]
    return tick_list


def write_ticks_to_bigquery(tick_type, tick_list, bigquery, BQ, bq_destination_table):
    """Writes ticks to BigQuery Table

    Args:
        tick_type: BID_AKS or TRADES
        tick_list: list with tick to be send to BigQuery
        BQ: BigQuery Client
        bq_destination_table: BigQuery destination table for write.

    Raises:
        ValueError: if tick_type not BID_ASK or TRADES
    """

    if tick_type not in ['BID_ASK', 'TRADES']:
        raise ValueError(tick_type_value_error(tick_type))

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'

    if tick_type == 'BID_ASK':
        columns = [
            'datetime', 'order', 'price_bid', 'size_bid', 'price_ask',
            'size_aks', 'bid_past_low', 'ask_past_high'
        ]

        job_config.schema = [
            bigquery.SchemaField('datetime', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('order', 'INT64', mode='REQUIRED'),
            bigquery.SchemaField('price_bid', 'FLOAT64', mode='NULLABLE'),
            bigquery.SchemaField('size_bid', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('price_ask', 'FLOAT64', mode='NULLABLE'),
            bigquery.SchemaField('size_aks', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('bid_past_low', 'BOOL', mode='NULLABLE'),
            bigquery.SchemaField('ask_past_high', 'BOOL', mode='NULLABLE')
        ]

    elif tick_type == 'TRADES':
        columns = [
            'datetime', 'order', 'price', 'size', 'exchange', 'special_conditions']

        job_config.schema = [
            bigquery.SchemaField('datetime', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('order', 'INT64', mode='REQUIRED'),
            bigquery.SchemaField('price', 'FLOAT64', mode='NULLABLE'),
            bigquery.SchemaField('size', 'INT64', mode='NULLABLE'),
            bigquery.SchemaField('exchange', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('special_conditions', 'STRING', mode='NULLABLE')
        ]

    df = pd.DataFrame(tick_list, columns=columns)
    BQ.load_table_from_dataframe(dataframe=df,
                                 destination=bq_destination_table,
                                 job_config=job_config)


def printProgress(ticks, tick_list):
    """ print progress during historical tick retrieval
    """

    print(
        f'{datetime.now().strftime("%H:%M:%S")} | {ticks[0].time.strftime("%Y-%m-%d %H:%M:%S")}-{ticks[-1].time.strftime("%H:%M:%S")} | {len(tick_list)} | {len(ticks)}')


def utc(datetime):
    """ Converts datetime to UTC datetime

    Args:
        datetime: timezone (un)aware datetime. If unaware local machines timezone is inferred.

    Returns:
        UTC datetime
    """

    utc = pytz.utc
    return utc.localize(datetime)


def split_list(a_list):
    """ splits a list into half
    Returns: two lists
    """
    half = len(a_list) // 2
    return a_list[:half], a_list[half:]


tick_dict = {
    'BID_ASK': 'BA',
    'TRADES': 'T'
}