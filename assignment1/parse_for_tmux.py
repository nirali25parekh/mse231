import json
import sys
import gzip
from datetime import datetime
import pandas as pd

# file = "tweets_work_sample.gz"
# file = "1hourtweet.gz"
# file = "nirali3.gz"
file = "24hr_work.gz"

with gzip.open(file,'rb') as fin:   
    
    count  = 0
    df = pd.DataFrame(columns=['date', 'tweet_type', 'tweet_text', 
                                   'og_author_id', 'og_author_name', 'og_author_desc', 'og_author_username', 
                                   'tweet_author_id', 'tweet_author_name', 'tweet_author_desc', 'tweet_author_username'])
    for tweet in fin:
        
        try:        
            tweet_d = json.loads(tweet)
    #         print(json.dumps(tweet_dict, indent=4))

            created_at = tweet_d['data']['created_at']
        
    #         extract date, hour, minute
            date = created_at[0:10]
            time = created_at[11:19]

            new_datetime = datetime.strptime(date+" " +time,'%Y-%m-%d %H:%M:%S')
            
            
            tweet_text = tweet_d['data']['text']
            if 'referenced_tweets' in tweet_d['data']:
                tweet_type = tweet_d['data']['referenced_tweets'][0]['type']
            else:
                tweet_type = None
                og_author_id = None
                og_author_name = None
                og_author_desc = None
                og_author_username = None
                
            
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
            
        
            new_row = pd.DataFrame({'datetime' : new_datetime, 
                            'tweet_type' : tweet_type, 
                            'tweet_text' : tweet_text,
                            'og_author_id' : og_author_id,
                            'og_author_name' : og_author_name,
                            'og_author_desc' : og_author_desc,
                            'og_author_username' : og_author_username,
                            'tweet_author_id' : tweet_author_id,
                            'tweet_author_name' : tweet_author_name,
                            'tweet_author_desc': tweet_author_desc,
                            'tweet_author_username': tweet_author_username
                        },  index=[0])
            df = pd.concat([new_row,df.loc[:]]).reset_index(drop=True)

            
    #         stop at 1000
            count += 1
            if count % 1000 == 0:
                print(count , "done!")
            # if count == 200:
            #     break
        except:
            print("Error occured")
        
# 
import datetime
df['rounded_datetime'] = df['datetime'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour,15*(dt.minute // 15)))

for d in df['rounded_datetime']:
    df['date'] = d.date()
    df['time'] = d.time()

df.to_csv('24hourtweets_tmux.csv', index = False)