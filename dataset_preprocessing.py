import pandas
import arff


# read file
df = pandas.read_csv(
    'Aug28_log',
    sep=' ',
    encoding='unicode_escape',
    header=None,
    na_values='-',
    error_bad_lines=False,
    names=['Client', 'Identifier', 'User', 'Timestamp', 'Time_Offset', 'Request', 'HTTP_status_code', 'Bytes'],
    dtype='str'
)

# cut the file to first 50000 records
df = df.iloc[:50000]

# drop unused columns
df.dropna(axis=1, how='all', inplace=True)

# handle timestamps
df['Timestamp'] = df['Timestamp'] + df['Time_Offset']
df.drop(['Time_Offset'], axis=1, inplace=True)
df['Timestamp'] = pandas.to_datetime(df['Timestamp'], format='[%d/%b/%Y:%H:%M:%S%z]')
df['Date'] = [d.date() for d in df['Timestamp']]
df['Time'] = [d.time() for d in df['Timestamp']]
df.drop(['Timestamp'], axis=1, inplace=True)
df['Date'] = df['Date'].astype('str')
df['Time'] = df['Time'].astype('str')

# handle bytes
df['Bytes'] = pandas.to_numeric(df['Bytes'], errors='coerce')
df['Bytes'].fillna(0, inplace=True)
df['Bytes'] = df['Bytes'].astype('int64')

# split requests
req = df['Request'].str.split(' ').str
df['Method'], df['Request_Url'], df['Request_Protocol'] = req[0], req[1], req[2]
df.drop('Request', axis=1, inplace=True)

# handle response codes
df['HTTP_status_code'] = pandas.to_numeric(df['HTTP_status_code'], errors='coerce')
df.dropna(inplace=True)
df['HTTP_status_code'] = df['HTTP_status_code'].astype('int64')

# printcheck
print(df.head(5).to_string())
print()
print(df.dtypes)
print()
print(df.isna().sum())

# save results to arff file
print("""
Since arff library does not provide support for python's date format thus
"Date" and "Time" have to be changed manually in arff file from string to:
"date YYYY-MM-dd" and "date HH:mm:ss"
""")
df = df[['Client', 'Date', 'Time', 'Method', 'Request_Url', 'Request_Protocol', 'HTTP_status_code', 'Bytes']]

# choose records with GET method and response code 200
df = df[(df['HTTP_status_code'] == 200) & (df['Method'] == 'GET')]

# drop records with graphic files
df = df[~df['Request_Url'].str.contains(
    '.jpg|.jpeg|.gif|.bmp|.xbm|.png|.mpg|.mpeg')]

arff.dump('Aug28_log_50k.arff',
          df.values,
          relation='Aug28_log_50k',
          names=df.columns)

df.to_csv('Aug28_log_50k.csv', index=False)
