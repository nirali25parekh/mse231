# should work with this -- zcat 1hourtweet.gz | python parse_tweets_stream.py



import json
import sys
import gzip
import csv
from datetime import datetime
import pandas as pd

count  = 0

header_names = ['datetime','date', 'time', 'rounded_datetime', 'tweet_type', 'tweet_text', 
                                   'og_author_id', 'og_author_name', 'og_author_desc', 'og_author_username', 
                                   'tweet_author_id', 'tweet_author_name', 'tweet_author_desc', 'tweet_author_username']

with open("output.csv", "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header_names)

# df = pd.DataFrame(columns=)

for tweet in sys.stdin:
    with open('output.csv', 'a', newline='', encoding='utf-8', errors='surrogatepass') as f:
    # print(line, end="")
          
        tweet_d = json.loads(tweet)
    #   print(json.dumps(tweet_dict, indent=4))

        created_at = tweet_d['data']['created_at']

    #   extract date, hour, minute
        date = created_at[0:10]
        time = created_at[11:19]

        
        
        dt = datetime.strptime(date+" " +time,'%Y-%m-%d %H:%M:%S')
        stringdt = dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # round off datetime
        rounded_datetime = datetime(dt.year, dt.month, dt.day, dt.hour,15*(dt.minute // 15))
        string_rounded_dt = rounded_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        # extract date and time separately from rounded
        date = rounded_datetime.date()
        stringdate = date.strftime('%Y-%m-%d')
        time = rounded_datetime.time()
        stringtime = time.strftime('%H:%M:%S')


        # assign tweet type
        tweet_text = tweet_d['data']['text']
        if 'referenced_tweets' in tweet_d['data']:
            tweet_type = tweet_d['data']['referenced_tweets'][0]['type']
        else:
            tweet_type = None
            og_author_id = None
            og_author_name = None
            og_author_desc = None
            og_author_username = None
            
        
        # separate OP and author
        tweet_author_id = tweet_d['data']['author_id']

        for i in tweet_d['includes']['users']:
            if i['id'] == tweet_author_id:
                tweet_author_id = i['id']
                tweet_author_name = i['name']
                tweet_author_desc = i['description']
                tweet_author_username = i['username']
            elif i['id'] != tweet_author_id:
                og_author_id = i['id']
                og_author_name = i['name']
                og_author_desc = i['description']
                og_author_username = i['username']


        array = [stringdt, stringdate, stringtime, string_rounded_dt, tweet_type, tweet_text, 
                    og_author_id, og_author_name, og_author_desc, og_author_username, 
                    tweet_author_id, tweet_author_name, tweet_author_desc, tweet_author_username]

        # write to csv file -- a row at a time
        writer = csv.writer(f, delimiter=",", )
        writer.writerow(array)


# stop at 1000
    count += 1
    if count % 10 == 0:
        print(count , "done!")
    if count == 200:
        break
    
    