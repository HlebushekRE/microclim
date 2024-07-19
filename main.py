from psycopg2 import connect
import pandas as pd


def get_data(name_korpus):
    conn = connect(host='localhost', database='zxc', user='postgres', password='12345')
    cur = conn.cursor()

    if name_korpus == 'D':
        query = "SELECT * FROM microclim_records WHERE device_id <= 20 OR device_id IN (78, 255, 257, 258)"
    elif name_korpus == 'A':
        query = "SELECT * FROM microclim_records WHERE device_id >= 33 AND device_id <= 69 OR device_id = 28"
    elif name_korpus == 'S':
        query = "SELECT * FROM microclim_records WHERE device_id >= 220 AND device_id <= 253 OR device_id IN (29, 30)"
    elif name_korpus == 'E':
        query = "SELECT * FROM microclim_records WHERE device_id >= 21 AND device_id <= 27 OR device_id IN (79, 80)"
    elif name_korpus == 'B':
        query = "SELECT * FROM microclim_records WHERE device_id >= 70 AND device_id <= 77"
    elif name_korpus == 'M':
        query = "SELECT * FROM microclim_records WHERE device_id >= 188 AND device_id <= 219"
    elif name_korpus == 'L':
        query = "SELECT * FROM microclim_records WHERE device_id >= 91 AND device_id <= 193"

    cur.execute(query)
    results = cur.fetchall()
    conn.close()

    return results


def date_format(df):
    df['date'] = df['date'].astype(str)
    df[['date', 'time']] = df['date'].str.split(' ', expand=True)
    df['date'] = pd.to_datetime(df['date']).dt.date

    grouped = df.groupby('date')
    list_of_dfs = [group for _, group in grouped]

    return list_of_dfs


def to_df(data):
    df = pd.DataFrame(data).drop([0, 2, 3, 8, 10, 11, 12, 13], axis=1, errors='warn') 
    # 0 - external_id, 2 - template_id, 3 - battery, 8 - powertype, 10 - tilt, 11 - type, 12 - device_id, 13 - id
    df.rename(columns={1: 'date', 4: 'co2', 5: 'hum', 6: 'lux', 7: 'noise', 9: 'temp'}, inplace=True)

    return df


# Корпус D (700k)
data = get_data('D')
df_D = to_df(data)

# Корпус E (152k)
data = get_data('E')
df_E = to_df(data)

# Корпус A (1168k)
data = get_data('A')
df_A = to_df(data)

# Корпус S (845k)
data = get_data('S')
df_S = to_df(data)

# Корпус B (244k)
data = get_data('B')
df_B = to_df(data)

# Корпус L (833k)
data = get_data('L')
df_L = to_df(data)

# Корпус M (799k)
data = get_data('M')
df_M = to_df(data)



df_E = date_format(df_E)
print(df_E[0])
