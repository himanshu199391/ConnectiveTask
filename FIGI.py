import json
import urllib.request
import urllib.parse
import pandas as pd
'''
See https://www.openfigi.com/api for more information.
'''

openfigi_apikey = 'df45204d-af8e-4753-9015-2ff4056da61a'  # Put API Key here
import csv

iso = pd.read_csv("ExchCode_ISOcountry.csv")
dict_iso_name = dict(zip(iso['EQUITY EXCH CODE'], iso['ISO COUNTRY']))




# jobs = [
#     {'idType': 'ID_SEDOL', 'idValue': 'BF4CP89'}
# ]


def map_jobs(jobs):
    '''
    Send an collection of mapping jobs to the API in order to obtain the
    associated FIGI(s).
    Parameters
    ----------
    jobs : list(dict)
        A list of dicts that conform to the OpenFIGI API request structure. See
        https://www.openfigi.com/api#request-format for more information. Note
        rate-limiting requirements when considering length of `jobs`.
    Returns
    -------
    list(dict)
        One dict per item in `jobs` list that conform to the OpenFIGI API
        response structure.  See https://www.openfigi.com/api#response-fomats
        for more information.
    '''
    handler = urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(handler)
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(jobs), encoding='utf-8'))
    request.add_header('Content-Type','application/json')
    if openfigi_apikey:
        request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
    request.get_method = lambda: 'POST'
    connection = opener.open(request)
    if connection.code != 200:
        raise Exception('Bad response')#('Bad response code {}'.format(str(response.status_code)))
    return json.loads(connection.read().decode('utf-8'))


def job_results_handler(jobs, job_results,dataframe):
    '''
    Handle the `map_jobs` results.  See `map_jobs` definition for more info.
    Parameters
    ----------
    jobs : list(dict)
        The original list of mapping jobs to perform.
    job_results : list(dict)
        The results of the mapping job.
    Returns
    -------
        None
    '''

    for job, result in zip(jobs, job_results):
        df = pd.DataFrame()
        job_str = '|'.join(job.values())
        composite_figis_str = [d['compositeFIGI'] for d in result.get('data', [])]
        ticker_figis_str = [d['ticker'] for d in result.get('data', [])]
        exchCode_figis_str = [d['exchCode'] for d in result.get('data', [])]
        id_str = [job['idValue']]*len(ticker_figis_str)
        df['Sedol'] = id_str
        df['Composite Figis'] = composite_figis_str
        df['Ticker'] = ticker_figis_str
        df['Exch Code'] = exchCode_figis_str
        df['Exch Code'] = df['Exch Code'].str.replace(' ', '')
        df=df.drop_duplicates(subset='Composite Figis', keep="last") #?
        df['Exch Code'] = df['Exch Code'].apply(lambda x: dict_iso_name[x])
        dataframe = dataframe.append(df)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe
    # dataframe.to_csv("check.csv",index=False)


        # result_str = figis_str or result.get('error')
        # output = '%s maps to FIGI(s) ->\n%s\n---' % (job_str, result_str)
        # print(output)


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

    # How many elements each




def main():
    '''
    Map the defined `jobs` and handle the results.
    Returns
    -------
        None
    '''
    dataframe_final = pd.DataFrame([])
    dataframe = pd.DataFrame([])


    with open('SEDOL sample.csv') as f:
        reader = list(csv.reader(f))
        # list should have
        df = pd.read_csv("SEDOL sample.csv")
        lis = list(df['SEDOL'].values)
        n = 100
        rows_chunk_100 = list(divide_chunks(lis, n))
        for rows in rows_chunk_100:
            jobs = []
            for row in rows:
                temp_dict = {}
                temp_dict['idValue'] = row
                temp_dict['idType'] = 'ID_SEDOL'
                jobs.append(temp_dict)
            job_results = map_jobs(jobs)
            dataframe_final = dataframe_final.append(job_results_handler(jobs, job_results,dataframe))
            dataframe_final = dataframe_final.reset_index(drop=True)
        dataframe_final.to_csv("final.csv")

main()