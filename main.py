from datetime import datetime
import shlex
import pandas as pd
import arff

TIME_THRESHOLD = 30*60 #30minutes in seconds
INGORE_ONE_WEBSITE_SESSION = True

# infile = open('res/Aug28_log.txt','r')
infile = open('res/logfile.txt', 'r')
# outfile = open('res/logfile.csv', 'w')

count = 1
extension_set = {"gif", "jpg", "jpeg","bmp", "xbm", "GIF", "JPG", 'wav'}
user_list = []
websites = {}
df = pd.DataFrame()
last_user_visit = {}
last_user_session_id = {}
next_session_id = 0

start_datetime = None
for line in infile.readlines():
    # print(line)
    try:
        lineaslist = shlex.split(line)
        if(len(lineaslist)!=8):
            print(lineaslist)
    except ValueError:
        continue
    out_list = []

    # 1: client
    user = lineaslist[0]
    out_list.append(user)

    raw_timestamp = lineaslist[3]
    time_and_date = datetime.strptime(raw_timestamp, '[%d/%b/%Y:%H:%M:%S') #'%b %d %Y %I:%M%p')
    if start_datetime is None:
        start_datetime = time_and_date

    # 2, 3: date, time
    out_list.append(str(time_and_date.date()))
    out_list.append(str(time_and_date.time()))

    method_req_protocol = lineaslist[5].split()
    if len(method_req_protocol)==2:
        method_req_protocol.append('') #empty string for unknown protocol
    elif len(method_req_protocol)>3:
        continue

    if method_req_protocol[0] != "GET":
        continue

    req = method_req_protocol[1]
    try:
        last_dot_index= req.rindex('.')
        extension_part= req[last_dot_index:]
        if "htm" in extension_part:
            pass
        elif any(ext in extension_part for ext in extension_set):
            continue
        # else:
        #     print(req[last_dot_index:])
    except ValueError:
        pass

    #deleting parameters
    try:
        question_mark_index = req.index('?')
        req = req[:question_mark_index]
    except ValueError:
        pass

    #counting visits on websites
    if req in websites:
        websites[req]+=1
    else:
        websites[req]=1

    # 4, 5, 6: method, request, protocol
    out_list+=method_req_protocol

    # 7: http status code
    status_code= int(lineaslist[6])
    if status_code != 200:
        continue
    out_list.append(status_code)

    # 8: bytes
    bytes = lineaslist[7]
    if bytes == '-':
        continue
    out_list.append(int(bytes))


    # for i, el in enumerate(out_list):
    #     if type(el)==str:
    #         out_list[i] = f'"{el}"'
    #     else:
    #         out_list[i] = str(el)

    # series = pd.Series(out_list)
    timestamp = (time_and_date-start_datetime).total_seconds()

    if user not in last_user_session_id:
        last_user_session_id[user] = next_session_id
        last_user_visit[user] = timestamp
        next_session_id+=1
    else:
        timebreak = timestamp-last_user_visit[user]
        if timebreak>TIME_THRESHOLD:
            last_user_session_id[user]=next_session_id
            next_session_id+=1
    current_session_id = last_user_session_id[user]
    last_user_visit[user] = timestamp

    user_list.append(user)

    series = pd.Series([user, req, timestamp, current_session_id])
    df = df.append(series, ignore_index=True)

    for i, el in enumerate(out_list):
        out_list[i] = str(el)

    out_str = ",".join(out_list)

    # outfile.write(out_str)
    # outfile.write('\n')
    count+=1
    if count>50000:
        break


infile.close()
# outfile.close()

# websites_count = len(websites)
# for website, occur in websites.items():
#     if occur>0.005*50000:
#         print(website, occur)



# print(websites_count)
print(df)

#how many times each website was visited
websites_and_visits = df[1].value_counts()
#how many visits in general
total_visits = websites_and_visits.sum()
#choose the most often visited sites
websites_and_visits = websites_and_visits[websites_and_visits > 0.005 * total_visits]
print(len(websites_and_visits))

# for i, row in df.iterrows():
#     print(row)

flagged_websites_names = websites_and_visits.index.values.tolist()

numeric_session_cols_names = ['session_time', 'actions', 'avg_time_per_site']
session_cols = numeric_session_cols_names + flagged_websites_names

session_df= pd.DataFrame(columns=session_cols)

for session in range(0,next_session_id):
    indexes_of_session = df[df[3]==session].index.values
    session_start = df.iloc[indexes_of_session[0]][2]
    session_end = df.iloc[indexes_of_session[-1]][2]
    session_time = int(session_end - session_start)
    if session_time == 0:
        continue
    actions = len(indexes_of_session)
    avg_time_per_site = int( session_time / (actions-1))

    flagged_websites = [False] * len(flagged_websites_names)
    for index in indexes_of_session:
        website_name=df.iloc[index][1]
        if website_name in flagged_websites_names:
            website_to_flag_index = flagged_websites_names.index(website_name)
            flagged_websites[website_to_flag_index] = True

    row = [session_time, actions, avg_time_per_site] + flagged_websites
    series = pd.Series(row, index=session_cols)
    session_df = session_df.append(series, ignore_index=True)

print(session_df)
arff.dump('out/session.arff', session_df.values,
          relation='session', names=session_df.columns)

user_cols = ["actions"] + flagged_websites_names
user_df= pd.DataFrame(columns=user_cols)
for i, user in enumerate(user_list):
    flagged_websites = [False]* len(flagged_websites_names)
    indexes_of_user = df[df[0] == user].index.values
    actions= len(indexes_of_user)
    for index in indexes_of_user:
        website_name = df.iloc[index][1]
        if website_name in flagged_websites_names:
            website_to_flag_index = flagged_websites_names.index(website_name)
            flagged_websites[website_to_flag_index] = True
    row = [actions] + flagged_websites
    series = pd.Series(row, index=user_cols)
    user_df=user_df.append(series, ignore_index=True)

arff.dump('out/user.arff', user_df.values,
          relation='user', names=user_df.columns)


session_discr_df = pd.DataFrame(columns=numeric_session_cols_names)

def discretisation(val, limit1, limit2):
    if val<limit1:
        return 'krotka'
    elif val<limit2:
        return "srednia"
    else:
        return 'dluga'

file=open("out/discretisation_limits.txt", 'w')
file.write("{'krotka','srednia','dluga'}\n")

for column in numeric_session_cols_names:
    short_middle = session_df[column].quantile(0.33)
    middle_long = session_df[column].quantile(0.66)
    file.write(f"progi dla {column}: {short_middle},{middle_long}\n")
    print(short_middle)
    session_df[column] = session_df[column].apply(lambda val:discretisation(val, short_middle, middle_long))


print(session_df)
arff.dump('out/session_discretisation.arff', session_df.values,
          relation='session_discretisation', names=session_df.columns)


