import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import json
import urllib.request
import urllib.parse
import pandas as pd
import base64
import io
from six.moves.urllib.parse import quote
import urllib.parse
import urllib
from io import StringIO
from flask import send_file


'''
See https://www.openfigi.com/api for more information.
'''

openfigi_apikey = 'df45204d-af8e-4753-9015-2ff4056da61a'  # Put API Key here
import csv
import dash_html_components as html
import dash_core_components as dcc
global dataframe_global
dataframe_global = pd.DataFrame()

def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    if 'csv' in filename:
        # Assume that the user uploaded a CSV file
        return pd.read_csv(
            io.StringIO(decoded.decode('utf-8')))
    elif 'xls' in filename:
        # Assume that the user uploaded an excel file
        return pd.read_excel(io.BytesIO(decoded))


def Header(app):
    return html.Div([get_header(app), html.Br([]), get_menu()])


def get_header(app):
    header = html.Div(
        [
            html.Div(
                [
                    html.Img(
                        src=app.get_asset_url("logo2.png"),
                        className="logo",
                    )
                ],
                className="row",
            ),
            html.Div(
                [
                    html.Div(
                        [html.H5("Connective Capital FIGI App")],
                        className="gs-text-header",
                    ),
                ],
                className="twelve columns",
                style={"padding-left": "0"},
            ),
        ],
        className="row",
    )
    return header


def get_menu():
    menu = html.Div(
        [
            dcc.Link(
                "Single Search",
                href="/FIGI/first",
                className="tab first",
            ),
            dcc.Link(
                "Multi-Inputs Search",
                href="/FIGI/second",
                className="tab",
            ),
        ],
        className="row all-tabs",
    )
    return menu




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
        dataframe = dataframe.append(df)
    dataframe = dataframe.reset_index(drop=True)
    return dataframe


def get_info(input_value):
    dataframe = pd.DataFrame()
    temp_dict = {}
    temp_dict['idValue'] = input_value
    temp_dict['idType'] = 'ID_SEDOL'
    job_results = map_jobs([temp_dict])
    dataframe_final = job_results_handler([temp_dict], job_results, dataframe)
    dataframe_final = dataframe_final.reset_index(drop=True)
    return dataframe_final

def layout_first(app):
    return(html.Div([html.Div([Header(app)]),
html.Div(
                [
    dcc.Input(id='my-id', value='B17KC69', type="text"),
    html.Button('Search', id='button'),
    dash_table.DataTable(id='datatable-first')],className="six columns")
],
        className="page",))

def layout_second(app):
    return(html.Div([html.Div([Header(app)]),
html.Div([
    dcc.Upload(
        id='datatable-upload',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%', 'height': '60px', 'lineHeight': '60px',
            'borderWidth': '1px', 'borderStyle': 'dashed',
            'borderRadius': '5px', 'textAlign': 'center', 'margin': '10px'
        },
    ),html.A("Download excel", href="/download_excel/"),
    dash_table.DataTable(id='datatable-upload-container'),
    # dcc.Graph(id='datatable-upload-graph')
],className="page")
],
        className="page",))

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"},],external_stylesheets=external_stylesheets
)
server = app.server

app.config.suppress_callback_exceptions = True


app.layout = html.Div(
    [dcc.Location(id="url", refresh=False), html.Div(id="page-content")]
)

# Update page
@app.server.route('/download_excel/')
def download_excel():
    #Create DF
    global dataframe_global
    df = dataframe_global
    #Convert DF
    strIO = io.BytesIO()
    excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
    df.to_excel(excel_writer, sheet_name="sheet1")
    excel_writer.save()
    excel_data = strIO.getvalue()
    strIO.seek(0)

    return send_file(strIO,
                     attachment_filename='test.xlsx',
                     as_attachment=True,cache_timeout=0)

@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname):
    if pathname == "/FIGI/first":
        return layout_first(app)
    elif pathname == "/FIGI/second":
        return layout_second(app)
    else :
        return layout_first(app)

@app.callback(
    [Output('datatable-first', 'data'),Output('datatable-first', 'columns')],
    [Input('button', 'n_clicks')],
    state=[State(component_id='my-id', component_property='value')]
)
def update_output_div(n_clicks, input_value):
    df = get_info(input_value)
    columns = [
        {"name": i, "id": i} for i in sorted(df.columns)
    ]
    return df.to_dict('records'),columns






@app.callback([Output('datatable-upload-container', 'data'),Output('datatable-upload-container', 'columns')],
              [Input('datatable-upload', 'contents')],
              [State('datatable-upload', 'filename')])
def update_output(contents, filename):
    global dataframe_global

    if contents is None:
        return [{}]
    dataframe = pd.DataFrame()
    dataframe_final = pd.DataFrame([])
    df = parse_contents(contents, filename)
    lis =list(df['SEDOL'].values)
    n = 100
    rows_chunk_100 = list(divide_chunks(lis, n))
    for rows in rows_chunk_100:
        jobs = []
        for row in rows:
            temp_dict = {}
            temp_dict['idValue'] = str(row)
            temp_dict['idType'] = 'ID_SEDOL'
            jobs.append(temp_dict)
        job_results = map_jobs(jobs)
        dataframe_final = dataframe_final.append(job_results_handler(jobs, job_results, dataframe))
        dataframe_final = dataframe_final.reset_index(drop=True)
    columns = [
        {"name": i, "id": i} for i in sorted(dataframe_final.columns)]
    dataframe_global = dataframe_final
    return dataframe_final.to_dict('records'),columns

@app.callback(
    [Output('download-link', 'href')],
    [Input('datatable-upload', 'contents')])
def update_download_link(filter_value):
    global dataframe_global
    dff = dataframe_global
    s = StringIO()
    csv_to_download = dff.to_csv(s)
    csv_string ="data: text / csv;charset = utf - 8, % EF % BB % BF"+urllib.quote(s.getvalue())
    return csv_string

if __name__ == '__main__':
    app.run_server(5008)