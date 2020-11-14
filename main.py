from datetime import datetime
import shlex
import pandas as pd



TIME_THRESHOLD = 30 #minutes
# infile = open('res/Aug28_log','rb')
infile = open('res/logfile.txt', 'r')
outfile = open('res/logfile.csv', 'w')

count = 1
extension_set = {"gif", "jpg", "jpeg","bmp", "xbm", "GIF", "JPG"}
user_set = {None}
websites = {}
df = pd.DataFrame()


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
    out_list.append(lineaslist[0])
    user_set.add(lineaslist[0])

    raw_timestamp = lineaslist[3]
    timestamp = datetime.strptime(raw_timestamp, '[%d/%b/%Y:%H:%M:%S') #'%b %d %Y %I:%M%p')
    # 2, 3: date, time
    out_list.append(str(timestamp.date()))
    out_list.append(str(timestamp.time()))

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

    for i, el in enumerate(out_list):
        out_list[i] = str(el)

    series = pd.Series(out_list)
    df = df.append(series, ignore_index=True)

    out_str = ",".join(out_list)

    outfile.write(out_str)
    outfile.write('\n')
    count+=1
    if count>100:
        break
print(len(user_set))
infile.close()
outfile.close()

websites_count = len(websites)
for website, occur in websites.items():
    if occur>0.005*50000:
        print(website, occur)



print(websites_count)
print(df)

websites_and_visits = df[4].value_counts()

for i, row in df.iterrows():
    pass
