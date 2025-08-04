import dash
from dash import dcc, html, Input, Output
import pandas as pd
import requests
import pyodbc
from datetime import datetime
import dash_bootstrap_components as dbc
from diskcache import Cache
import redis
import os
import sys
sys.path.insert(0, "./.python_packages/lib/python3.10/site-packages")
from concurrent.futures import ThreadPoolExecutor, as_completed

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Cache setup: Azure Redis if REDIS_URL set, else local diskcache
REDIS_URL = os.getenv('REDIS_URL')
if REDIS_URL:
    cache = redis.Redis.from_url(REDIS_URL)
else:
    cache = Cache("cache")  # Local fallback

# Environment variables for Azure
API_TOKEN = os.getenv('API_TOKEN', "cc9493e2-395e-4d59-a2b9-ebed87bc2471")
API_PASSWORD = os.getenv('API_PASSWORD', "Welcome1")
API_SITENAME = os.getenv('API_SITENAME', "fivestar")
API_USERID = os.getenv('API_USERID', "jeff.thompson")
SQL_SERVER = os.getenv('SQL_SERVER', "SQL-03")
SQL_DATABASE = os.getenv('SQL_DATABASE', "TECHSYS")

# CSS for scrolling alert feed (save as assets/style.css)
scrolling_css = """
.alert-feed-container {
    width: 80%;
    margin: 10px auto;
    overflow: hidden;
    white-space: nowrap;
    position: relative;
    height: 30px;
    border: 1px solid #ddd;
    background-color: #fff3cd;
}
.alert-feed {
    display: inline-block;
    animation: scroll 20s linear infinite;
    padding-left: 100%;
}
@keyframes scroll {
    0% { transform: translateX(0); }
    100% { transform: translateX(-100%); }
}
.alert-item {
    display: inline-block;
    margin-right: 50px;
    color: #d9534f;
    font-size: 14px;
}
"""

# Function to fetch location codes from SQL Server
def get_location_codes():
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        query = "SELECT LOCATION_NAME, LOCATION_CODE FROM T_LOCATION WHERE LOCATION_ACTIVE = 'Y'"
        df_locations = pd.read_sql(query, conn)
        conn.close()
        
        df_locations['brand'] = df_locations['LOCATION_NAME'].apply(
            lambda x: 'Blaze Pizza' if x.startswith('BZ') else 
                      'Five Guys USA' if x.startswith(('FG - OR', 'FG - WA')) else 
                      'Five Guys Canada'
        )
        print(f"Retrieved {len(df_locations)} locations from SQL")
        return df_locations[['LOCATION_CODE', 'LOCATION_NAME', 'brand']]
    except Exception as e:
        print(f"Error fetching location codes: {e}")
        return pd.DataFrame(columns=['LOCATION_CODE', 'LOCATION_NAME', 'brand'])

# Function to fetch API data for a single location
def fetch_location_data(location_code, location_name, brand, today):
    try:
        url = f"https://webservices.net-chef.com/salesmix/v1/getAllSalesMix?includeDetails=true&locationCode={location_code}&posNumber=POS&transactionDate={today}"
        headers = {
            "accept": "application/json",
            "authenticationtoken": API_TOKEN,
            "password": API_PASSWORD,
            "sitename": API_SITENAME,
            "userid": API_USERID
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(f"Raw API Data for {location_code}:", data)
        
        headers_data = [item['salesMixHeaderDetails'] for item in data]
        df = pd.DataFrame(headers_data)
        
        if 'location' not in df.columns:
            df['location'] = location_name
        df['location_code'] = location_code
        df['brand'] = brand
        df['refresh_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return df
    except Exception as e:
        print(f"Error fetching data for {location_code}: {e} - Ignoring and continuing")
        return pd.DataFrame({
            'error': [str(e)],
            'location': [location_name],
            'location_code': [location_code],
            'brand': [brand],
            'transactionDate': [pd.NaT],
            'chargedTips': [0.0],
            'endingCount': [0],
            'totalNetSales': [0.0],
            'paidOuts': [0.0],
            'bookCash': [0.0],
            'overShort': [0.0],
            'refresh_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })

# Function to fetch and process API data for all locations
def fetch_data():
    cache_key = "sales_data"
    try:
        cached_data = cache.get(cache_key) if isinstance(cache, Cache) else cache.get(cache_key.encode()).decode('utf-8') if cache.exists(cache_key) else None
        if cached_data is not None:
            print("Using cached data")
            df = pd.read_json(cached_data) if isinstance(cached_data, str) else cached_data
            print(f"Cached DataFrame shape: {df.shape}, columns: {df.columns}")
            return df
    except Exception as e:
        print(f"Cache read error: {e}")
    
    try:
        df_locations = get_location_codes()
        if df_locations.empty:
            raise ValueError("No location codes retrieved from SQL Server")
        
        today = datetime.now().strftime('%d-%b-%y')
        all_data = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(fetch_location_data, str(row['LOCATION_CODE']), row['LOCATION_NAME'], row['brand'], today)
                for _, row in df_locations.iterrows()
            ]
            for future in as_completed(futures):
                all_data.append(future.result())
        
        df = pd.concat(all_data, ignore_index=True)
        print(f"Raw combined DataFrame shape: {df.shape}, columns: {df.columns}")
        
        df['transactionDate'] = pd.to_datetime(df['transactionDate'])
        df['refresh_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Filter out $0 sales
        df = df[df['totalNetSales'] != 0.0]
        print(f"After $0 filter DataFrame shape: {df.shape}")
        
        if df.empty:
            print("Warning: DataFrame empty after filtering $0 sales")
            return pd.DataFrame({
                'error': ["No valid sales data after filtering"],
                'location': ['Unknown'],
                'location_code': ['Unknown'],
                'brand': ['Unknown'],
                'transactionDate': [pd.NaT],
                'chargedTips': [0.0],
                'endingCount': [0],
                'totalNetSales': [0.0],
                'paidOuts': [0.0],
                'bookCash': [0.0],
                'overShort': [0.0],
                'refresh_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            })
        
        columns = ['location', 'location_code', 'brand', 'transactionDate', 'chargedTips', 
                  'endingCount', 'totalNetSales', 'paidOuts', 'bookCash', 'overShort', 'refresh_time']
        if not all(col in df.columns for col in columns[:-1]):  # Exclude refresh_time from check
            print(f"Warning: Missing columns: {df.columns}")
        
        # Cache data for 15 min
        cache.set(cache_key, df[columns].to_json() if isinstance(cache, redis.Redis) else df[columns], expire=900)
        return df[columns]
    except Exception as e:
        print(f"Error in fetch_data: {e}")
        return pd.DataFrame({
            'error': [str(e)],
            'location': ['Unknown'],
            'location_code': ['Unknown'],
            'brand': ['Unknown'],
            'transactionDate': [pd.NaT],
            'chargedTips': [0.0],
            'endingCount': [0],
            'totalNetSales': [0.0],
            'paidOuts': [0.0],
            'bookCash': [0.0],
            'overShort': [0.0],
            'refresh_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        })

# Initial data load
df = fetch_data()

app.layout = html.Div(style={'backgroundColor': '#f4f4f9', 'padding': '20px', 'fontFamily': 'Arial'}, children=[
    html.H1('Sales Dashboard', style={'textAlign': 'center', 'color': '#1f77b4'}),
    html.Div(id='refresh-time', style={'textAlign': 'center', 'fontSize': '14px', 'color': '#666', 'marginBottom': '20px'}),
    html.H3('Alerts', style={'textAlign': 'center', 'color': '#d9534f'}),
    html.Div(id='alert-feed', className='alert-feed-container', style={'width': '80%', 'margin': '10px auto', 'overflow': 'hidden', 'white-space': 'nowrap', 'position': 'relative', 'height': '30px', 'border': '1px solid #ddd', 'backgroundColor': '#fff3cd'}),
    html.H3('Brand Metrics', style={'textAlign': 'center', 'color': '#1f77b4'}),
    html.Table(id='brand-metrics-table', style={'width': '80%', 'margin': '10px auto', 'border': '1px solid #ddd'}),
    html.H3('Top 10 Locations by Sales', style={'textAlign': 'center', 'color': '#1f77b4'}),
    html.Table(id='top-sales-table', style={'width': '80%', 'margin': '10px auto', 'border': '1px solid #ddd'}),
    html.H3('Bottom 10 Locations by Sales', style={'textAlign': 'center', 'color': '#1f77b4'}),
    html.Table(id='bottom-sales-table', style={'width': '80%', 'margin': '10px auto', 'border': '1px solid #ddd'}),
    dcc.Interval(id='interval-component', interval=15*60*1000, n_intervals=0),
    dcc.Interval(id='alert-cycle', interval=5*1000, n_intervals=0)
])

@app.callback(
    [Output('brand-metrics-table', 'children'), Output('top-sales-table', 'children'),
     Output('bottom-sales-table', 'children'), Output('alert-feed', 'children'),
     Output('refresh-time', 'children')],
    [Input('interval-component', 'n_intervals'), Input('alert-cycle', 'n_intervals')]
)
def update_dashboard(n, alert_n):
    global df
    df = fetch_data()
    
    if 'error' in df.columns:
        alert_children = [html.Div("Error fetching data: " + df['error'].iloc[0], className='alert-feed')]
        return [[], [], [], alert_children, "Error occurred"]
    
    # Aggregate by brand
    brand_metrics = df.groupby('brand').agg({
        'totalNetSales': 'sum',
        'endingCount': 'sum',
        'chargedTips': 'sum'
    }).reset_index()
    
    # Top 10 and bottom 10 locations by sales
    top_10 = df.nlargest(10, 'totalNetSales')[['location', 'brand', 'totalNetSales']]
    bottom_10 = df.nsmallest(10, 'totalNetSales')[['location', 'brand', 'totalNetSales']]
    
    # Alerts for overShort > +$30 or < -$30
    overshort_alerts = df[df['overShort'].abs() > 30][['location', 'overShort']]
    alert_items = []
    if not overshort_alerts.empty:
        for _, row in overshort_alerts.iterrows():
            alert_items.append(html.Span(f"{row['location']}: Over/Short ${row['overShort']:.2f}", className='alert-item'))
    else:
        alert_items = [html.Span("No Over/Short alerts (±$30)", className='alert-item', style={'color': '#5cb85c'})]
    
    alert_index = alert_n % len(alert_items) if alert_items else 0
    alert_children = [html.Div([alert_items[alert_index]], className='alert-feed')]
    
    # Brand metrics table
    brand_rows = [
        html.Tr([html.Th(col, style={'border': '1px solid #ddd', 'padding': '8px'}) for col in 
                ['Brand', 'Total Net Sales', 'Total Customers', 'Total Tips']])
    ]
    for _, row in brand_metrics.iterrows():
        brand_rows.append(html.Tr([
            html.Td(row['brand'], style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(f"${row['totalNetSales']:.2f}", style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(row['endingCount'], style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(f"${row['chargedTips']:.2f}", style={'border': '1px solid #ddd', 'padding': '8px'})
        ]))
    
    # Top 10 sales table
    top_rows = [
        html.Tr([html.Th(col, style={'border': '1px solid #ddd', 'padding': '8px'}) for col in 
                ['Location', 'Brand', 'Total Net Sales']])
    ]
    for _, row in top_10.iterrows():
        top_rows.append(html.Tr([
            html.Td(row['location'], style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(row['brand'], style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(f"${row['totalNetSales']:.2f}", style={'border': '1px solid #ddd', 'padding': '8px'})
        ]))
    
    # Bottom 10 sales table
    bottom_rows = [
        html.Tr([html.Th(col, style={'border': '1px solid #ddd', 'padding': '8px'}) for col in 
                ['Location', 'Brand', 'Total Net Sales']])
    ]
    for _, row in bottom_10.iterrows():
        bottom_rows.append(html.Tr([
            html.Td(row['location'], style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(row['brand'], style={'border': '1px solid #ddd', 'padding': '8px'}),
            html.Td(f"${row['totalNetSales']:.2f}", style={'border': '1px solid #ddd', 'padding': '8px'})
        ]))
    
    refresh_text = f"Last refreshed: {df['refresh_time'].iloc[0] if 'refresh_time' in df.columns else 'Unknown'} | Data for {len(df)} locations"
    
    return brand_rows, top_rows, bottom_rows, alert_children, refresh_text

if __name__ == '__main__':
    app.run_server(debug=True)

