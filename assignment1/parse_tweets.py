# author -- Nirali Parekh -- nirali25@stanford.edu
# Assignment 1 - MS&E 231

# Team 4: Nirali Parekh, Roberto Lobato, Dinesh Moorjani

# should work with this? TODO-- -- zcat 1hourtweet.gz | python parse_tweets_stream.py

import json
import sys
from datetime import datetime
import pandas as pd

for tweet in sys.stdin:
          
        tweet_d = json.loads(tweet)
    #   print(json.dumps(tweet_dict, indent=4))

        created_at = tweet_d['data']['created_at']

    #   extract date, hour, minute
        date = created_at[0:10]
        time = created_at[11:19]

        
        
        dt = datetime.strptime(date+" "+time,'%Y-%m-%d %H:%M:%S')
        stringdt = dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # round off datetime
        rounded_datetime = datetime(dt.year, dt.month, dt.day, dt.hour,15*(dt.minute // 15))
        string_rounded_dt = rounded_datetime.strftime('%Y-%m-%d %H:%M:%S')
        
        # extract date and time separately from rounded
        # date = rounded_datetime.date()
        # stringdate = date.strftime('%Y-%m-%d')
        # time = rounded_datetime.time()
        # stringtime = time.strftime('%H:%M:%S')


        # assign tweet type
        if 'referenced_tweets' in tweet_d['data']:
            tweet_type = tweet_d['data']['referenced_tweets'][0]['type']
        else:
            tweet_type = ""
            og_author_name = ""
            og_author_desc = ""
            
        
        # separate OP and author
        tweet_author_id = tweet_d['data']['author_id']

        if 'includes' not in tweet_d:
            continue
        else:
            for i in tweet_d['includes']['users']:
                if i['id'] == tweet_author_id:
                    tweet_author_name = i['name'].replace(",", " ").replace("\n", " ")
                    tweet_author_desc = i['description'].replace(",", " ").replace("\n", " ")
                elif i['id'] != tweet_author_id:
                    og_author_name = i['name'].replace(",", " ").replace("\n", " ")
                    og_author_desc = i['description'].replace(",", " ").replace("\n", " ")

            array = [stringdt, string_rounded_dt, tweet_type, og_author_name, og_author_desc, tweet_author_name, tweet_author_desc]

            print(",".join(array))

