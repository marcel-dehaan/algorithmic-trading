import datetime
import logging
import os
import re
import traceback
from datetime import datetime, timedelta

import pandas as pd
import pytz
from bs4 import BeautifulSoup as bs
from google.cloud import bigquery
from google.cloud import error_reporting
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage

PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'test'
BQ_TABLE = 'a'

ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'uspr_streaming_error')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'uspr_streaming_success')
CNPR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'cnpr_streaming')
SEC_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'sec_streaming')
UNKNOWN_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'unknown_streaming')
DUPLICATE_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID,
                                             'uspr-streaming-duplicate')
NO_TICKER_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID,
                                             'uspr_streaming_no_ticker')

CS = storage.Client()
ER = error_reporting.Client()
BQ = bigquery.Client()
FS = firestore.Client()
PS = pubsub_v1.PublisherClient()


def streaming(data, context):
    """This function is executed whenever
    a file is added to Cloud Storage
    """
    bucket_name = data['bucket']
    file_name = data['name']
    blob = CS.get_bucket(bucket_name).blob(file_name)

    if '_USPR_' in file_name:
        db_ref = FS.collection(u'uspr_duplicate_check')\
            .document(u'%s' % file_name)

        if _was_already_ingested(db_ref):
            _handle_duplication(db_ref)
        else:
            try:
                print(f'starting scraper: {_now()}')
                df = _scraper(blob, file_name)
                print(f'finished scraping: {_now()}')
                if df is not None:
                    print(f'inserting into bq: {_now()}')
                    _insert_into_bigquery(df)
                    print(f'inserted into bq: {_now()}')
                    _handle_success(df, db_ref)
                    print(f'handled success: {_now()}')
                else:
                    _handle_no_ticker(file_name)
            except:
                _handle_error(db_ref)
                ER.report_exception()

    elif 'CNPR' or 'CANADA' in file_name:
        _handle_cnpr(file_name)

    elif 'SEC' in file_name:
        _handle_sec(file_name)

    else:
        _handle_unknown(file_name)


def _was_already_ingested(db_ref):
    status = db_ref.get()
    return status.exists and 'success' in status.to_dict()


def _now():
    return datetime.utcnow()\
        .replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


def _handle_cnpr(file_name):
    message = 'CNPR streaming file: \'%s\'' % file_name
    PS.publish(CNPR_TOPIC,
               message.encode('utf-8'),
               file_name=file_name)


def _handle_sec(file_name):
    message = 'SEC streaming file: \'%s\'' % file_name
    PS.publish(SEC_TOPIC,
               message.encode('utf-8'),
               file_name=file_name)


def _handle_no_ticker(file_name):
    message = 'No ticker streaming file: \'%s\'' % file_name
    PS.publish(NO_TICKER_TOPIC,
               message.encode('utf-8'),
               file_name=file_name)


def _handle_unknown(file_name):
    message = 'Unknown streaming file: \'%s\'' % file_name
    PS.publish(UNKNOWN_TOPIC,
               message.encode('utf-8'),
               file_name=file_name)


def _handle_success(df, db_ref):
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)

    db_ref = FS.collection(u'uspr_30s_todo').document(u'%s' % db_ref.id)
    doc = {
        u'ticker': df['ticker'][0],
        u'exchange': df['exchange'][0],
        u'publish_dt': df['publish_dt'][0],
        u'received_dt': df['received_dt'][0],
        u'distributor': df['distributor'][0],
        u'IS_codes': df['IS_codes'][0],
        u'title': df['title'][0],
        u'body': df['body_text'][0]
    }
    db_ref.set(doc)

    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    PS.publish(SUCCESS_TOPIC,
               message.encode('utf-8'),
               file_name=db_ref.id)
    logging.info(message)


def _handle_duplication(db_ref):
    dups = [_now()]
    data = db_ref.get().to_dict()
    if 'duplication_attempts' in data:
        dups.extend(data['duplication_attempts'])
    db_ref.update({
        'duplication_attempts': dups
    })
    message = 'Duplicate streaming file: \'%s\'' % db_ref.id
    print(message)
    PS.publish(DUPLICATE_TOPIC,
               message.encode('utf-8'),
               file_name=db_ref.id)


def _handle_error(db_ref):
    message = 'Error streaming file \'%s\'. Cause: %s' \
              % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    logging.error(message)
    PS.publish(ERROR_TOPIC,
               message.encode('utf-8'),
               file_name=db_ref.id
               )


def _scraper(blob, filename):
    soup = bs(blob.download_as_string(), 'xml')
    title = soup.document.nitf.head.title.get_text() \
        if soup.document.nitf.head.find('title') else None
    language = soup.find('xn:language').get_text() \
        if soup.find('xn:language') else None

    body_head = soup.document.body.find('body.head')
    body_content = soup.document.body.find('body.content')
    headlines = []
    for hl in ['hl1', 'hl2', 'hl3']:
        try:
            headlines.append(body_head.hedline.find(hl).get_text())
        except:
            break

    try:
        distributor = body_head.distributor.get_text()
    except:
        ER.report_exception()
        distributor = ''

    try:
        body_text = \
            ''.join([p.get_text().replace('\xa0', '')
                     for p in body_content.find('div', {'class': 'xn-content'})
                     .find_all('p') if p.get_text() not in ['', ' ', '\xa0']])
    except:
        ER.report_exception()
        body_text = ''

    try:
        exchange, ticker = \
            soup.find('xn:companyCode') \
            .get_text().split('#', 1)[0].split(':', 1)

        if len(exchange.split('-', 1)) > 1:
            exchange = exchange.split('-', 1)[0]
    except:
        ER.report_exception()
        return None

    try:
        publish_dt = soup.find('xn:publicationTime').get_text()
        publish_dt = datetime.strptime(publish_dt,
                                       '%Y-%m-%dT%H:%M:%S%z').astimezone()
    except Exception as e:
        ER.report_exception()
        publish_dt = ''

    try:
        received_dt = \
            [x.get_text() for x in soup.find_all('vendorData')
             if 'Special Code=PC/t' in x.get_text()][0].split('t.')[-1]
        received_dt = datetime.strptime(received_dt, '%y%m%d%H%M%S%f')
        received_dt += timedelta(hours=int(soup.find('xn:publicationTime')
                                           .get_text()[-4]))
        received_dt = pytz.utc.localize(received_dt)

    except:
        try:
            received_dt = soup.find('xn:receivedTime').get_text()
            received_dt = datetime.strptime(received_dt,
                                            '%Y-%m-%dT%H:%M:%S%z').astimezone()
        except:
            ER.report_exception()
            received_dt = ''

    IS = {'type': 'subjectCode', 're': '^IS', 'codes': []}

    for code in [IS]:
        [code['codes'].append(c.text.split('#', 1)[0].split('/', 1)[1])
         for c in soup.find_all(code['type']) if re.compile(code['re'])
         .match(c.text.split('#', 1)[0])]

    data = [[
        received_dt, publish_dt, ticker, exchange, title,
        distributor, headlines, IS['codes'], body_text,
        language, filename
    ]]

    columns = [
        'received_dt', 'publish_dt', 'ticker', 'exchange', 'title',
        'distributor', 'headlines', 'IS_codes', 'body_text',
        'language', 'filename'
    ]

    return pd.DataFrame(data=data, columns=columns)


def _insert_into_bigquery(df):
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    try:
        job = BQ.load_table_from_dataframe(dataframe=df,
                                           destination=table,
                                           job_config=job_config).result()
    except:
        ER.report_exception()
        return None
