import gzip
import json

# input via stdin
import sys
# for line in sys.stdin:
#     print(line, end="")

from gzip import GzipFile

# class GzipWrap(object):
#     # input is a filelike object that feeds the input
#     def __init__(self, input, filename = None):
#         self.input = input
#         self.buffer = ''
#         self.zipper = GzipFile(filename, mode = 'wb', fileobj = self)

#     def read(self, size=-1):
#         if (size < 0) or len(self.buffer) < size:
#             for s in self.input:
#                 self.zipper.write(s)
#                 if size > 0 and len(self.buffer) >= size:
#                     self.zipper.flush()
#                     break
#             else:
#                 self.zipper.close()
#             if size < 0:
#                 ret = self.buffer
#                 self.buffer = ''
#         else:
#             ret, self.buffer = self.buffer[:size], self.buffer[size:]
#         return ret

#     def flush(self):
#         pass

#     def write(self, data):
#         self.buffer += data

#     def close(self):
#         self.input.close()

# print("Done!")

# gz files --> from 7zip open in vscode
# put comma before every {"data"... and put entire thing in array
    
    
    # four columns of the output are: (1) date; (2) time rounded to the nearest 15-minute interval (e.g., 18:30);
    #  (3) the name of the user; and (4) the name of the original poster, if the tweet is a retweet or quoted retweet (otherwise 'NA').

serial, pydata, jdata = None, None, None
    
file = import gzip

# file = "tweets_work_sample.gz"
file = "1hourtweet.gz"

with gzip.open(file,'rb') as fin:        
    for line in fin:        
        print('got line', line)
        
with open('tweets_work_sample.gz', 'rb') as f:
    serial = gzip.decompress(f.read())
    
jdata = serial.decode('utf-8')

pydata = json.loads(jdata)

print(pydata)


    # should work with command:  zcat tweets.gz | python3 parse_tweets.py