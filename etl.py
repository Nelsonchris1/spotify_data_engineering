import requests
import psycopg2
from sqlalchemy import create_engine
import json
import pandas as pd
import datetime

userID = "rzjdx0uxvfojeqam5v8urwinh"
TOKEN = "BQDleJ9g1vMnPux146knIMe5SK8C_lxYFS_BJcWCoylKAXSCP4dxuecQCE-mTXRKb5BWbzwpdjGyzeCKqblYuxEwYUWPWT8CasE_RtPmec9aDOndwcLHk8zsWhE4uVND2BC_7lHoADDddr6FM2SZ5p_OiExWfh1v5e7u2JiT"

create_table = """CREATE TABLE IF NOT EXISTS spotify(
                song varchar,
                artist varchar,
                played_at varchar PRIMARY KEY, 
                timestamp varchar)"""
insert_into_db = """
                INSERT INTO spotifyDB (song, artist, played_at, timestamp) VALUES(%, %, %, %)
"""



def CreateTable(create_variable):
    try:
        curr = create_engine('postgresql://postgres:blessing@localhost:5432/spotifyDB')
        curr.execute(create_variable)
        print('Table created')
        curr.dispose()
    except:
        print('Connection failure')
    

def InsertTable(df:pd.DataFrame):
    try:
        curr = create_engine('postgresql://postgres:blessing@localhost:5432/spotifyDB')
        df.to_sql('spotify', curr, index=False, if_exists='append')
        print('Data inserted')
        curr.dispose()
    except:
        print("Data exsist already")

def data_validation(df:pd.DataFrame) -> bool:
    if df.empty:
        print("No songs listened in the last 3 days. Execution completed!!")

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")    

    if df.isnull().values.any():
        raise  ("Null value found")


    today = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()) * 1000 
    timestamps = df['timestamp']

    for timestamp in timestamps:
        if today >= int((datetime.datetime.strptime(timestamp, '%Y-%m-%d')).timestamp() * 1000):
            pass
        else:
            raise Exception("At least one song did'nt come in the last 3 days")
    
    return True


if __name__ == "__main__":
    
    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=3)
    yesterday_unix = (int(yesterday.timestamp()) * 1000)
    
   
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix), headers = header)

    data = r.json()

    song_name =[]
    artist_name = []
    played_at_list = []
    timestamp = []

    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamp.append(song["played_at"][0:10])

    data_for_table = {"song": song_name,
                      "artist": artist_name,
                     "played_at": played_at_list,
                     "timestamp": timestamp}

    table = pd.DataFrame(data_for_table, columns = ['song', 'artist', 'played_at', 'timestamp'])
    
    
    if data_validation(df=table):
        print("Data Validated, move to load stage")

    
    CreateTable(create_table)
    InsertTable(table)


    
