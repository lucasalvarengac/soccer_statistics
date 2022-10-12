import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import yaml
from yaml.loader import SafeLoader

# Open the file and load the file
with open('.env.yaml') as f:
    config = yaml.load(f, Loader=SafeLoader)
    print(config)

JSON_FILE=config.get('GOOGLE_APPLICATION_CREDENTIALS')
GOOGLE_SHEETS_NAME=config.get('GOOGLE_SHEETS_NAME')

print(JSON_FILE)

def get_html_data(season):
    """
    Get html data from transfermarkt.com
    """
    base_url='https://www.transfermarkt.com/campeonato-brasileiro-serie-a/gesamtspielplan/wettbewerb/BRA1?saison_id={season}&spieltagVon=1&spieltagBis=38'
    headers={'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
    html_content=requests.get(base_url.format(season=season), headers=headers).content
    print(pd.read_html(html_content))
    return pd.concat(pd.read_html(html_content)[4:])

def parse_data(data):
    """"
    Parse the html data and return a list of dictionaries
    """
    data=data.loc[data.iloc[:,4].str.contains('\d+\:\d+$')]
    print(data)
    data.columns=['date', 'time', 'home_teamm', 'home1', 'score', 'home2', 'away', 'away_team', 'score', 'attendance', 'referee']
    data['date']=data.date.fillna(method='ffill')
    data['time']=data.time.fillna(method='ffill')
    data['home_team']=data.home_teamm.str.replace('\(\d+\.\)(.*)', '\\1')
    data['home_team_score']=data['score'].iloc[:,0].str.extract('(\d+)\:\d+').astype(int)
    data['home_team_pos']=data.home_teamm.str.replace('[^\d]+', '').astype(int)
    data['away_team']=data.away.str.replace('\(\d+\.\)(.*)', '\\1')
    data['away_team_score']=data['score'].iloc[:,0].str.extract('\d+\:(\d+)').astype(int)
    data['away_team_pos']=data.away.str.replace('[^\d]+', '').astype(int)
    data['date']=pd.to_datetime(data.date, format='%a %m/%d/%y').dt.strftime('%Y-%m-%d')
    data['time']=pd.to_datetime(data.time, format='%H:%M %p').dt.time.astype(str)
    data=data.drop(['home1', 'home2', 'score', 'away', 'attendance', 'referee', 'home_teamm'], axis=1)
    return data

def write_gsheets(data):
    """
    Write data to google sheets
    """
    scope=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials=ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    client=gspread.authorize(credentials)
    sheet=client.open(title=GOOGLE_SHEETS_NAME).worksheet('Página1')
    print(sheet.get_all_records())
    sheet.clear()
    sheet.update([data.columns.values.tolist()]+data.values.tolist())



if __name__ == '__main__':
    full_data=[parse_data(get_html_data(season)) for season in range(2005, 2022)]
    full_data=pd.concat(full_data)
    write_gsheets(full_data)
    print('Done')
    