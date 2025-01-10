# Written by: Maryam Adebayo
# Part 1 & 2 & 3



### Part 1


#1A
import urllib.request # Importing the urllib library to handle HTTP requests
import time
tweetData = "http://dbgroup.cdm.depaul.edu/DSC450/OneDayOfTweets.txt" 
webFD = urllib.request.urlopen(tweetData) # Open a connection to the URL

#150000
start_a150 = time.time()
f1 = open("OneDayOfTweets_150.txt", "w") # Open a local file for writing the tweets
import json
for i in range(150000):
   content = webFD.readline()  # Read a single line from the web data
   f1.write(f"{content}") # Write the line to the local file

f1.close() # Close the local file after writing is complete
end_a150 = time.time()
run_a150 = end_a150 - start_a150 # Calculate and print the runtime
print(run_a150)

#750000
start_a750 = time.time()
f2 = open("OneDayOfTweets_750.txt", "w")
import json
for i in range(750000):
   content = webFD.readline()
   f2.write(f"{content}")
f2.close()
end_a750 = time.time()
run_a750 = end_a750 - start_a750 
print(run_a750)







   
#1B

import sqlite3
import time
import json

# saving tweets to file, populate the 3-table schema created in SQLite

#150000
conn = sqlite3.connect('DSC450.db')
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS Users")
cursor.execute("DROP TABLE IF EXISTS Tweets")
cursor.execute("DROP TABLE IF EXISTS Geo")

createtbl_users = """
CREATE TABLE Users (
    id              VARCHAR2(255) PRIMARY KEY,
    name            VARCHAR2(255),
    screen_name     VARCHAR2(255),
    description     VARCHAR2(4000), 
    friends_count   VARCHAR2(255)
);
"""

createtbl_tweets = """
CREATE TABLE Tweets (
    user_id                     VARCHAR2(255),
    created_at                  VARCHAR2(255),
    id_str                      VARCHAR2(255) PRIMARY KEY,
    text                        VARCHAR2(4000), 
    source                      VARCHAR2(255),
    in_reply_to_user_id         VARCHAR2(255),
    in_reply_to_screen_name     VARCHAR2(255),
    in_reply_to_status_id       VARCHAR2(255),
    retweet_count               NUMBER, 
    contributors                VARCHAR2(4000), 
    geo_id                      VARCHAR2(255),
    
    FOREIGN KEY (user_id) REFERENCES Users (id)
);
"""

createtbl_geo = """
CREATE TABLE Geo (
    geo_id          VARCHAR2(255) PRIMARY KEY,
    type            VARCHAR2(255),
    longitude       VARCHAR2(255),
    latitude        VARCHAR2(4000), 
    

    FOREIGN KEY (geo_id) REFERENCES Tweets (geo_id)
);
"""

cursor.execute(createtbl_users)
cursor.execute(createtbl_tweets)
cursor.execute(createtbl_geo)

start_b110 = time.time()
tweets = []
for i in range(150000):
    for words in webFD:
        try:
          tdict = json.loads(words.decode('utf8'))
          tweets.append(json.loads(words.decode('utf8')))
          
          if tdict['geo'] is not None:
                 values_users = (
                   tdict['user']['id'],
                   tdict['user']['name'], 
                   tdict['user']['screen_name'], 
                   tdict['user']['description'], 
                   tdict['user']['friends_count']
                 )
          
              
                 values_tweets = (
                   tdict['user']['id'],
                   tdict['created_at'], 
                   tdict['id_str'], 
                   tdict['text'], 
                   tdict['source'], 
                   tdict['in_reply_to_user_id'], 
                   tdict['in_reply_to_screen_name'], 
                   tdict['in_reply_to_status_id'], 
                   tdict['retweet_count'], 
                   tdict['contributors'],
                   str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1])
                 )
                 
              
                 values_geo = (
                   str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1]),
                   tdict['geo']['type'],
                   tdict['geo']['coordinates'][0],
                   tdict['geo']['coordinates'][1] 
                 )
              
                 cursor.execute("""
                    INSERT OR IGNORE INTO Users(
                      id,              
                      name,            
                      screen_name,     
                      description,      
                      friends_count
                     ) VALUES (
                     ?, ?, ?, ?, ?
                     )
                   """, (values_users[0],values_users[1], values_users[2], values_users[3], values_users[4]))

                 cursor.execute("""
                 INSERT OR IGNORE INTO Geo(
                    geo_id,
                    type,
                    longitude,
                    latitude
                  ) VALUES ( ?, ?, ?, ?
                  )
                 """,(values_geo[0],values_geo[1], values_geo[2], values_geo[3]))
                 
                 cursor.execute("""
                 INSERT OR IGNORE INTO Tweets(
                    user_id,
                    created_at, 
                    id_str, 
                    text, 
                    source, 
                    in_reply_to_user_id, 
                    in_reply_to_screen_name, 
                    in_reply_to_status_id, 
                    retweet_count, 
                    contributors,
                    geo_id
                    
                 ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                 )
                 """,(values_tweets[0],values_tweets[1], values_tweets[2], values_tweets[3], values_tweets[4], values_tweets[5], values_tweets[6], values_tweets[7],values_tweets[8], values_tweets[9], values_tweets[10]))
        
        except ValueError:
            pass
            
            

   
print(cursor.execute("SELECT COUNT(DISTINCT id) FROM Users").fetchall())  # Query to count distinct user IDs in the Users table and print the result
print(cursor.execute("SELECT COUNT(DISTINCT id_str) FROM Tweets").fetchall())
print(cursor.execute("SELECT COUNT(DISTINCT geo_id) FROM Geo").fetchall())
end_b110 = time.time()
run_b110 = end_b110 - start_b110
print("Elapsed time for b110:" + str(run_b110) + 'seconds')



#750000
cursor.execute("DROP TABLE IF EXISTS Users")
cursor.execute("DROP TABLE IF EXISTS Tweets")
cursor.execute("DROP TABLE IF EXISTS Geo")

createtbl_users = """
CREATE TABLE Users (
   id              VARCHAR2(255) PRIMARY KEY,
   name            VARCHAR2(255),
   screen_name     VARCHAR2(255),
   description     VARCHAR2(4000), 
   friends_count   VARCHAR2(255)
);
"""

createtbl_tweets = """
CREATE TABLE Tweets (
   user_id                     VARCHAR2(255),
   created_at                  VARCHAR2(255),
   id_str                      VARCHAR2(255) PRIMARY KEY,
   text                        VARCHAR2(4000), 
   source                      VARCHAR2(255),
   in_reply_to_user_id         VARCHAR2(255),
   in_reply_to_screen_name     VARCHAR2(255),
   in_reply_to_status_id       VARCHAR2(255),
   retweet_count               NUMBER, 
   contributors                VARCHAR2(4000), 
   geo_id                      VARCHAR2(255),
   
   FOREIGN KEY (user_id) REFERENCES Users (id)
);
"""

createtbl_geo = """
CREATE TABLE Geo (
   geo_id          VARCHAR2(255) PRIMARY KEY,
   type            VARCHAR2(255),
   longitude       VARCHAR2(255),
   latitude        VARCHAR2(4000), 
   

   FOREIGN KEY (geo_id) REFERENCES Tweets (geo_id)
);
"""

cursor.execute(createtbl_users)
cursor.execute(createtbl_tweets)
cursor.execute(createtbl_geo)
start_b550 = time.time()
tweets = []
for i in range(750000):
   for words in webFD:
       try:
         tdict = json.loads(words.decode('utf8'))
         #tweets.append(json.loads(words.decode('utf8')))
         
         if tdict['geo'] is not None:
                values_users = (
                  tdict['user']['id'],
                  tdict['user']['name'], 
                  tdict['user']['screen_name'], 
                  tdict['user']['description'], 
                  tdict['user']['friends_count']
                )
         
             
                values_tweets = (
                  tdict['user']['id'],
                  tdict['created_at'], 
                  tdict['id_str'], 
                  tdict['text'], 
                  tdict['source'], 
                  tdict['in_reply_to_user_id'], 
                  tdict['in_reply_to_screen_name'], 
                  tdict['in_reply_to_status_id'], 
                  tdict['retweet_count'], 
                  tdict['contributors'],
                  str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1])
                )
                
             
                values_geo = (
                  str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1]),
                  tdict['geo']['type'],
                  tdict['geo']['coordinates'][0],
                  tdict['geo']['coordinates'][1] 
                )
             
                cursor.execute("""
                   INSERT OR IGNORE INTO Users(
                     id,              
                     name,            
                     screen_name,     
                     description,      
                     friends_count
                    ) VALUES (
                    ?, ?, ?, ?, ?
                    )
                  """, (values_users[0],values_users[1], values_users[2], values_users[3], values_users[4]))

                cursor.execute("""
                INSERT OR IGNORE INTO Geo(
                   geo_id,
                   type,
                   longitude,
                   latitude
                 ) VALUES ( ?, ?, ?, ?
                 )
                """,(values_geo[0],values_geo[1], values_geo[2], values_geo[3]))
                
                cursor.execute("""
                INSERT OR IGNORE INTO Tweets(
                   user_id,
                   created_at, 
                   id_str, 
                   text, 
                   source, 
                   in_reply_to_user_id, 
                   in_reply_to_screen_name, 
                   in_reply_to_status_id, 
                   retweet_count, 
                   contributors,
                   geo_id
                   
                ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,(values_tweets[0],values_tweets[1], values_tweets[2], values_tweets[3], values_tweets[4], values_tweets[5], values_tweets[6], values_tweets[7],values_tweets[8], values_tweets[9], values_tweets[10]))
       
       except ValueError:
           pass
           
           

  
print(cursor.execute("SELECT COUNT(DISTINCT id) FROM Users").fetchall())
print(cursor.execute("SELECT COUNT(DISTINCT id_str) FROM Tweets").fetchall())
print(cursor.execute("SELECT COUNT(DISTINCT geo_id) FROM Geo").fetchall())
end_b550 = time.time()
run_b550 = end_b550 - start_b550
print("Elapsed time b550:" + str(run_b550) + 'seconds')
      









#1C
import sqlite3
import time
import matplotlib as plt

150000
cursor.execute("DROP TABLE IF EXISTS Users")
cursor.execute("DROP TABLE IF EXISTS Tweets")
cursor.execute("DROP TABLE IF EXISTS Geo")

createtbl_users = """
CREATE TABLE Users (
   id              VARCHAR2(255) PRIMARY KEY,
   name            VARCHAR2(255),
   screen_name     VARCHAR2(255),
   description     VARCHAR2(4000), 
   friends_count   VARCHAR2(255)
);
"""

createtbl_tweets = """
CREATE TABLE Tweets (
   user_id                     VARCHAR2(255),
   created_at                  VARCHAR2(255),
   id_str                      VARCHAR2(255) PRIMARY KEY,
   text                        VARCHAR2(4000), 
   source                      VARCHAR2(255),
   in_reply_to_user_id         VARCHAR2(255),
   in_reply_to_screen_name     VARCHAR2(255),
   in_reply_to_status_id       VARCHAR2(255),
   retweet_count               NUMBER, 
   contributors                VARCHAR2(4000), 
   geo_id                      VARCHAR2(255),
   
   FOREIGN KEY (user_id) REFERENCES Users (id)
);
"""

createtbl_geo = """
CREATE TABLE Geo (
   geo_id          VARCHAR2(255) PRIMARY KEY,
   type            VARCHAR2(255),
   longitude       VARCHAR2(255),
   latitude        VARCHAR2(4000), 
   

   FOREIGN KEY (geo_id) REFERENCES Tweets (geo_id)
);
"""

cursor.execute(createtbl_users)
cursor.execute(createtbl_tweets)
cursor.execute(createtbl_geo)


start_c110 = time.time()
with open("OneDayOfTweets_110.txt", "r") as fd:
   for line in fd:
       try:
           tdict = json.loads(line)
           if tdict['geo'] is not None:
                        values_users = (
                          tdict['user']['id'],
                          tdict['user']['name'], 
                          tdict['user']['screen_name'], 
                          tdict['user']['description'], 
                          tdict['user']['friends_count']
                        )
                 
                     
                        values_tweets = (
                          tdict['user']['id'],
                          tdict['created_at'], 
                          tdict['id_str'], 
                          tdict['text'], 
                          tdict['source'], 
                          tdict['in_reply_to_user_id'], 
                          tdict['in_reply_to_screen_name'], 
                          tdict['in_reply_to_status_id'], 
                          tdict['retweet_count'], 
                          tdict['contributors'],
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1])
                        )
                        
                     
                        values_geo = (
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1]),
                          tdict['geo']['type'],
                          tdict['geo']['coordinates'][0],
                          tdict['geo']['coordinates'][1] 
                        )
                     
                        cursor.execute("""
                           INSERT OR IGNORE INTO Users(
                             id,              
                             name,            
                             screen_name,     
                             description,      
                             friends_count

                            ) VALUES (
                            ?, ?, ?, ?, ?
                            )
                          """, (values_users[0],values_users[1], values_users[2], values_users[3], values_users[4]))

                        cursor.execute("""
                        INSERT OR IGNORE INTO Geo(
                           geo_id,
                           type,
                           longitude,
                           latitude
                         ) VALUES ( ?, ?, ?, ?
                         )
                        """,(values_geo[0],values_geo[1], values_geo[2], values_geo[3]))
                        
                        cursor.execute("""
                        INSERT OR IGNORE INTO Tweets(
                           user_id,
                           created_at, 
                           id_str, 
                           text, 
                           source, 
                           in_reply_to_user_id, 
                           in_reply_to_screen_name, 
                           in_reply_to_status_id, 
                           retweet_count, 
                           contributors,
                           geo_id
                           
                        ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,(values_tweets[0],values_tweets[1], values_tweets[2], values_tweets[3], values_tweets[4], values_tweets[5], values_tweets[6], values_tweets[7],values_tweets[8], values_tweets[9], values_tweets[10]))
       except ValueError:
               pass
end_c110 = time.time() 
run_c110 = end_c110 - start_c110
print("Elapsed time for c110:" + str(run_c110) + 'seconds')



# 550000
cursor.execute("DROP TABLE IF EXISTS Users")
cursor.execute("DROP TABLE IF EXISTS Tweets")
cursor.execute("DROP TABLE IF EXISTS Geo")

createtbl_users = """
CREATE TABLE Users (
   id              VARCHAR2(255) PRIMARY KEY,
   name            VARCHAR2(255),
   screen_name     VARCHAR2(255),
   description     VARCHAR2(4000), 
   friends_count   VARCHAR2(255)
);
"""

createtbl_tweets = """
CREATE TABLE Tweets (
   user_id                     VARCHAR2(255),
   created_at                  VARCHAR2(255),
   id_str                      VARCHAR2(255) PRIMARY KEY,
   text                        VARCHAR2(4000), 
   source                      VARCHAR2(255),
   in_reply_to_user_id         VARCHAR2(255),
   in_reply_to_screen_name     VARCHAR2(255),
   in_reply_to_status_id       VARCHAR2(255),
   retweet_count               NUMBER, 
   contributors                VARCHAR2(4000), 
   geo_id                      VARCHAR2(255),
   
   FOREIGN KEY (user_id) REFERENCES Users (id)
);
"""

createtbl_geo = """
CREATE TABLE Geo (
   geo_id          VARCHAR2(255) PRIMARY KEY,
   type            VARCHAR2(255),
   longitude       VARCHAR2(255),
   latitude        VARCHAR2(4000), 
   

   FOREIGN KEY (geo_id) REFERENCES Tweets (geo_id)
);
"""

cursor.execute(createtbl_users)
cursor.execute(createtbl_tweets)
cursor.execute(createtbl_geo)

start_c550 = time.time()
with open("OneDayOfTweets_550.txt", "r") as fd:
   for line in fd:
       try:
           tdict = json.loads(line)
           if tdict['geo'] is not None:
                        values_users = (
                          tdict['user']['id'],
                          tdict['user']['name'], 
                          tdict['user']['screen_name'], 
                          tdict['user']['description'], 
                          tdict['user']['friends_count']
                        )
                 
                     
                        values_tweets = (
                          tdict['user']['id'],
                          tdict['created_at'], 
                          tdict['id_str'], 
                          tdict['text'], 
                          tdict['source'], 
                          tdict['in_reply_to_user_id'], 
                          tdict['in_reply_to_screen_name'], 
                          tdict['in_reply_to_status_id'], 
                          tdict['retweet_count'], 
                          tdict['contributors'],
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1])
                        )
                        
                     
                        values_geo = (
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1]),
                          tdict['geo']['type'],
                          tdict['geo']['coordinates'][0],
                          tdict['geo']['coordinates'][1] 
                        )
                     
                        cursor.execute("""
                           INSERT OR IGNORE INTO Users(
                             id,              
                             name,            
                             screen_name,     
                             description,      
                             friends_count
                            ) VALUES (
                            ?, ?, ?, ?, ?
                            )
                          """, (values_users[0],values_users[1], values_users[2], values_users[3], values_users[4]))

                        cursor.execute("""
                        INSERT OR IGNORE INTO Geo(
                           geo_id,
                           type,
                           longitude,
                           latitude
                         ) VALUES ( ?, ?, ?, ?
                         )
                        """,(values_geo[0],values_geo[1], values_geo[2], values_geo[3]))
                        
                        cursor.execute("""
                        INSERT OR IGNORE INTO Tweets(
                           user_id,
                           created_at, 
                           id_str, 
                           text, 
                           source, 
                           in_reply_to_user_id, 
                           in_reply_to_screen_name, 
                           in_reply_to_status_id, 
                           retweet_count, 
                           contributors,
                           geo_id
                           
                        ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,(values_tweets[0],values_tweets[1], values_tweets[2], values_tweets[3], values_tweets[4], values_tweets[5], values_tweets[6], values_tweets[7],values_tweets[8], values_tweets[9], values_tweets[10]))
       except ValueError:
               pass
end_c550 = time.time()
run_c550 = end_c550 - start_c550
print("Elapsed time for c550:" + str(run_c550) + 'seconds')









#1D
import sqlite3
import json

conn = sqlite3.connect('DSC450.db')
cursor = conn.cursor()
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS Users")
cursor.execute("DROP TABLE IF EXISTS Tweets")
cursor.execute("DROP TABLE IF EXISTS Geo")

createtbl_users = """
CREATE TABLE Users (
   id              VARCHAR2(255) PRIMARY KEY,
   name            VARCHAR2(255),
   screen_name     VARCHAR2(255),
   description     VARCHAR2(4000), 
   friends_count   VARCHAR2(255)
);
"""

createtbl_tweets = """
CREATE TABLE Tweets (
   user_id                     VARCHAR2(255),
   created_at                  VARCHAR2(255),
   id_str                      VARCHAR2(255) PRIMARY KEY,
   text                        VARCHAR2(4000), 
   source                      VARCHAR2(255),
   in_reply_to_user_id         VARCHAR2(255),
   in_reply_to_screen_name     VARCHAR2(255),
   in_reply_to_status_id       VARCHAR2(255),
   retweet_count               NUMBER, 
   contributors                VARCHAR2(4000), 
   geo_id                      VARCHAR2(255),
   
   FOREIGN KEY (user_id) REFERENCES Users (id)
);
"""

createtbl_geo = """
CREATE TABLE Geo (
   geo_id          VARCHAR2(255) PRIMARY KEY,
   type            VARCHAR2(255),
   longitude       VARCHAR2(255),
   latitude        VARCHAR2(4000), 
   

   FOREIGN KEY (geo_id) REFERENCES Tweets (geo_id)
);
"""

cursor.execute(createtbl_users)
cursor.execute(createtbl_tweets)
cursor.execute(createtbl_geo)

#110000
start_d110 = time.time()
v_users = []
v_tweets = []
v_geo = []
cnt = 0
with open("OneDayOfTweets_110.txt", "r") as fd:
   for line in fd:
       try:
           tdict = json.loads(line)
           if tdict['geo'] is not None:
                        values_users = (
                          tdict['user']['id'],
                          tdict['user']['name'], 
                          tdict['user']['screen_name'], 
                          tdict['user']['description'], 
                          tdict['user']['friends_count']
                        )
                        v_users.append(values_users)
                        
                        values_tweets = (
                          tdict['user']['id'],
                          tdict['created_at'], 
                          tdict['id_str'], 
                          tdict['text'], 
                          tdict['source'], 
                          tdict['in_reply_to_user_id'], 
                          tdict['in_reply_to_screen_name'], 
                          tdict['in_reply_to_status_id'], 
                          tdict['retweet_count'], 
                          tdict['contributors'],
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1])
                        )
                        v_tweets.append(values_tweets)
                     
                        values_geo = (
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1]),
                          tdict['geo']['type'],
                          tdict['geo']['coordinates'][0],
                          tdict['geo']['coordinates'][1] 
                        )
                        v_geo.append(values_geo)
                        cnt += 1
                        
           if cnt == 2500:
               for i in v_users:
                        cursor.execute("""
                           INSERT OR IGNORE INTO Users(
                             id,              
                             name,            
                             screen_name,     
                             description,      
                             friends_count
                            ) VALUES (
                            ?, ?, ?, ?, ?
                            )
                          """, (values_users[0],values_users[1], values_users[2], values_users[3], values_users[4]))
                        
               for i in v_tweets:
                        cursor.execute("""
                        INSERT OR IGNORE INTO Geo(
                           geo_id,
                           type,
                           longitude,
                           latitude
                         ) VALUES ( ?, ?, ?, ?
                         )
                        """,(values_geo[0],values_geo[1], values_geo[2], values_geo[3]))

               for i in v_geo:
                        cursor.execute("""
                        INSERT OR IGNORE INTO Tweets(
                           user_id,
                           created_at, 
                           id_str, 
                           text, 
                           source, 
                           in_reply_to_user_id, 
                           in_reply_to_screen_name, 
                           in_reply_to_status_id, 
                           retweet_count, 
                           contributors,
                           geo_id
                           
                        ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,(values_tweets[0],values_tweets[1], values_tweets[2], values_tweets[3], values_tweets[4], values_tweets[5], values_tweets[6], values_tweets[7],values_tweets[8], values_tweets[9], values_tweets[10]))
               v_users = []
               v_tweets = []
               v_geo = []
               cnt = 0
       except ValueError:
               pass
end_d110 = time.time()
run_d110 = end_d110 - start_d110
print("Elapsed time for d110:" + str(run_d110) + 'seconds')




# 550000
cursor.execute("DROP TABLE IF EXISTS Users")
cursor.execute("DROP TABLE IF EXISTS Tweets")
cursor.execute("DROP TABLE IF EXISTS Geo")

createtbl_users = """
CREATE TABLE Users (
   id              VARCHAR2(255) PRIMARY KEY,
   name            VARCHAR2(255),
   screen_name     VARCHAR2(255),
   description     VARCHAR2(4000), 
   friends_count   VARCHAR2(255)
);
"""

createtbl_tweets = """
CREATE TABLE Tweets (
   user_id                     VARCHAR2(255),
   created_at                  VARCHAR2(255),
   id_str                      VARCHAR2(255) PRIMARY KEY,
   text                        VARCHAR2(4000), 
   source                      VARCHAR2(255),
   in_reply_to_user_id         VARCHAR2(255),
   in_reply_to_screen_name     VARCHAR2(255),
   in_reply_to_status_id       VARCHAR2(255),
   retweet_count               NUMBER, 
   contributors                VARCHAR2(4000), 
   geo_id                      VARCHAR2(255),
   
   FOREIGN KEY (user_id) REFERENCES Users (id)
);
"""

createtbl_geo = """
CREATE TABLE Geo (
   geo_id          VARCHAR2(255) PRIMARY KEY,
   type            VARCHAR2(255),
   longitude       VARCHAR2(255),
   latitude        VARCHAR2(4000), 
   

   FOREIGN KEY (geo_id) REFERENCES Tweets (geo_id)
);
"""

cursor.execute(createtbl_users)
cursor.execute(createtbl_tweets)
cursor.execute(createtbl_geo)

start_d550 = time.time()
v_users = []
v_tweets = []
v_geo = []
cnt = 0
with open("OneDayOfTweets_550.txt", "r") as fd:
   for line in fd:
       try:
           tdict = json.loads(line)
           if tdict['geo'] is not None:
                        values_users = (
                          tdict['user']['id'],
                          tdict['user']['name'], 
                          tdict['user']['screen_name'], 
                          tdict['user']['description'], 
                          tdict['user']['friends_count']
                        )
                        v_users.append(values_users)
                        
                        values_tweets = (
                          tdict['user']['id'],
                          tdict['created_at'], 
                          tdict['id_str'], 
                          tdict['text'], 
                          tdict['source'], 
                          tdict['in_reply_to_user_id'], 
                          tdict['in_reply_to_screen_name'], 
                          tdict['in_reply_to_status_id'], 
                          tdict['retweet_count'], 
                          tdict['contributors'],
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1])
                        )
                        v_tweets.append(values_tweets)
                     
                        values_geo = (
                          str(tdict['geo']['coordinates'][0]) + ';' + str(tdict['geo']['coordinates'][1]),
                          tdict['geo']['type'],
                          tdict['geo']['coordinates'][0],
                          tdict['geo']['coordinates'][1] 
                        )
                        v_geo.append(values_geo)
                        cnt += 1
                        
           if cnt == 2500:
               for i in v_users:
                        cursor.execute("""
                           INSERT OR IGNORE INTO Users(
                             id,              
                             name,            
                             screen_name,     
                             description,      
                             friends_count
                            ) VALUES (
                            ?, ?, ?, ?, ?
                            )
                          """, (values_users[0],values_users[1], values_users[2], values_users[3], values_users[4]))
                        
               for i in v_tweets:
                        cursor.execute("""
                        INSERT OR IGNORE INTO Geo(
                           geo_id,
                           type,
                           longitude,
                           latitude
                         ) VALUES ( ?, ?, ?, ?
                         )
                        """,(values_geo[0],values_geo[1], values_geo[2], values_geo[3]))

               for i in v_geo:
                        cursor.execute("""
                        INSERT OR IGNORE INTO Tweets(
                           user_id,
                           created_at, 
                           id_str, 
                           text, 
                           source, 
                           in_reply_to_user_id, 
                           in_reply_to_screen_name, 
                           in_reply_to_status_id, 
                           retweet_count, 
                           contributors,
                           geo_id
                           
                        ) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,(values_tweets[0],values_tweets[1], values_tweets[2], values_tweets[3], values_tweets[4], values_tweets[5], values_tweets[6], values_tweets[7],values_tweets[8], values_tweets[9], values_tweets[10]))
               v_users = []
               v_tweets = []
               v_geo = []
               cnt = 0
       except ValueError:
               pass
end_d550 = time.time()
run_d550 = end_d550 - start_d550
print("Elapsed time for d550:" + str(run_d550) + 'seconds')





import numpy as np
import matplotlib.pyplot as plt

data = {'Run a - 150':run_a150, 'Run a - 750':run_a750, 'Run b - 110':run_b110, 
       'Run b - 550':run_b550, 'Run c - 110':run_c110, 'Run c - 550':run_c550, 'Run d - 110':run_d110, 
       'Run d - 550':run_d550}
courses = list(data.keys())
values = list(data.values())
 
fig = plt.figure(figsize = (20, 10))

# creating the bar plot
plt.bar(courses, values, color ='maroon', 
       width = 0.4)

plt.xlabel("Runs for each section")
plt.ylabel("Time (s)")
plt.title("Runtimes")
plt.show()

      
### Part 2


#2A  

cursor.execute("""
SELECT 
   Tweets.user_id, 
   MIN(Geo.longitude) AS MinLongitude, 
   AVG(Geo.longitude) AS AvgLongitude,
   MIN(Geo.latitude) AS MinLatitude, 
   AVG(Geo.latitude) AS AvgLatitude
FROM 
   Tweets
JOIN 
   Geo ON Tweets.geo_id = Geo.geo_id
GROUP BY 
   Tweets.user_id;
""")



#2B

start1 = time.time()
for i in range(1,5):
   cursor.execute("""
   SELECT 
       Tweets.user_id, 
       MIN(Geo.longitude) AS MinLongitude, 
       AVG(Geo.longitude) AS AvgLongitude,
       MIN(Geo.latitude) AS MinLatitude, 
       AVG(Geo.latitude) AS AvgLatitude
   FROM 
       Tweets
   JOIN 
       Geo ON Tweets.geo_id = Geo.geo_id
   GROUP BY 
       Tweets.user_id;
   """)
end1 = time.time()
print("Elapsed time for executed query 5x:" + str(end1-start1) + 'seconds')




start2 = time.time()
for i in range(1, 25):
   cursor.execute("""
   SELECT 
       Tweets.user_id, 
       MIN(Geo.longitude) AS MinLongitude, 
       AVG(Geo.longitude) AS AvgLongitude,
       MIN(Geo.latitude) AS MinLatitude, 
       AVG(Geo.latitude) AS AvgLatitude
   FROM 
       Tweets
   JOIN 
       Geo ON Tweets.geo_id = Geo.geo_id
   GROUP BY 
       Tweets.user_id;
   """)
end2 = time.time()
print("Elapsed time executed query 25x:" + str(end2-start2) + 'seconds')




#2C
from statistics import mean
lst1 =[]
for val in tweets: 
 if val in lst1:  # Check if the tweet is already processed
   continue # Skip if it is already processed
 else:
   if val['geo'] is not None:
       long = [val['geo']['coordinates'][0]] # Extract longitude
       lat = [val['geo']['coordinates'][1]]
       lst1.append((val['user']['id'], min(long), min(lat), mean(long), mean(lat))) # Add user ID and geo stats to the list


#2D
from statistics import mean
start_d5 = time.time()
for i in range(1,5):
   lst1 =[]
   for val in tweets: 
     if val in lst1: 
       continue 
     else:
       if val['geo'] is not None:
           long = [val['geo']['coordinates'][0]]
           lat = [val['geo']['coordinates'][1]]
           lst1.append((val['user']['id'], min(long), min(lat), mean(long), mean(lat)))
end_d5 = time.time()
run_d5 = end_d5 - start_d5
print("Elapsed time for python code to run 5x:" + str(run_d5) + 'seconds')



from statistics import mean
start_d25 = time.time()
for i in range(1,25):
   lst1 =[]
   for val in tweets: 
     if val in lst1: 
       continue 
     else:
       if val['geo'] is not None:
           long = [val['geo']['coordinates'][0]]
           lat = [val['geo']['coordinates'][1]]
           lst1.append((val['user']['id'], min(long), min(lat), mean(long), mean(lat)))
end_d25 = time.time()
run_d25 = end_d25 - start_d25
print("Elapsed time for python code to run 25x:" + str(run_d25) + 'seconds')

import numpy as np # Import numpy for numerical operations
import matplotlib.pyplot as plt  # Import matplotlib for visualization

data = {'Run 5x ':run_d5, 'Run 25x - 550':run_d25} # Dictionary to hold runtimes
courses = list(data.keys())
values = list(data.values())


# Create a figure with specifications
fig = plt.figure(figsize = (20, 10))


plt.bar(courses, values, color ='maroon', 
       width = 0.4)

plt.xlabel("Runs for each section")
plt.ylabel("Time (s)")
plt.title("Runtimes")
plt.show()





### Part 3


# 3A
start1 = time.time()
cursor.execute('DROP TABLE IF EXISTS FullTable;')
cursor.execute('''
CREATE TABLE FullTable AS
SELECT
   Tweets.id_str,
   Tweets.text,
   Tweets.user_id,
   Geo.longitude,
   Geo.latitude,
   Users.name AS user_name,
   Users.screen_name,
   Users.description,
   Users.friends_count,
   Tweets.created_at,
   Tweets.source,
   Tweets.in_reply_to_user_id,
   Tweets.in_reply_to_screen_name,
   Tweets.in_reply_to_status_id,
   Tweets.retweet_count,
   Tweets.contributors,
   Geo.type AS geo_type
FROM
   Tweets
LEFT JOIN
   Geo ON Tweets.geo_id = Geo.geo_id
LEFT JOIN
   Users ON Tweets.user_id = Users.id

UNION ALL

SELECT
   Tweets.id_str,
   Tweets.text,
   Tweets.user_id,
   Geo.longitude,
   Geo.latitude,
   Users.name AS user_name,
   Users.screen_name,
   Users.description,
   Users.friends_count,
   Tweets.created_at,
   Tweets.source,
   Tweets.in_reply_to_user_id,
   Tweets.in_reply_to_screen_name,
   Tweets.in_reply_to_status_id,
   Tweets.retweet_count,
   Tweets.contributors,
   Geo.type AS geo_type
FROM
   Geo
LEFT JOIN
   Tweets ON Geo.geo_id = Tweets.geo_id
LEFT JOIN
   Users ON Tweets.user_id = Users.id
WHERE
   Tweets.id_str IS NULL

UNION ALL

SELECT
   Tweets.id_str,
   Tweets.text,
   Tweets.user_id,
   Geo.longitude,
   Geo.latitude,
   Users.name AS user_name,
   Users.screen_name,
   Users.description,
   Users.friends_count,
   Tweets.created_at,
   Tweets.source,
   Tweets.in_reply_to_user_id,
   Tweets.in_reply_to_screen_name,
   Tweets.in_reply_to_status_id,
   Tweets.retweet_count,
   Tweets.contributors,
   Geo.type AS geo_type
FROM
   Users
LEFT JOIN
   Tweets ON Users.id = Tweets.user_id
LEFT JOIN
   Geo ON Tweets.geo_id = Geo.geo_id
WHERE
   Tweets.id_str IS NULL;
''')
conn.commit()
end1 = time.time()
print("Elapsed time to create table:" + str(end1-start1) + 'seconds')

start1 = time.time()
print(cursor.execute("SELECT COUNT(*) FROM FullTable").fetchall())
end1 = time.time()
print("Elapsed time to run query:" + str(end1-start1) + 'seconds')

#3B & 3C
cursor = conn.cursor()
cursor.execute('SELECT * FROM Tweets')
tweets = cursor.fetchall()
tweets_json = [
   {
       "user_id": row[0],
       "created_at": row[1],
       "id_str": row[2],
       "text": row[3],
       "source": row[4],
       "in_reply_to_user_id": row[5],
       "in_reply_to_screen_name": row[6],
       "in_reply_to_status_id": row[7],
       "retweet_count": row[8],
       "contributors": row[9],
       "geo_id": row[10]
   }
   for row in tweets
]

with open('tweets.json', 'w') as f:
   json.dump(tweets_json, f)

with open('tweets.csv', 'w',  encoding='utf-8') as f:
   headers = [description[0] for description in cursor.description]
   f.write(','.join(headers) + '\n')
   
   for tweet in tweets:
       f.write(','.join(map(str, tweet)) + '\n')




cursor.execute('SELECT * FROM FullTable')
fullTable = cursor.fetchall()
fullTable_json = [
   {
       "tweet_id": row[0],
       "tweet_text": row[1],
       "user_id": row[2],
       "longitude": row[3],
       "latitude": row[4],
       "user_name": row[5],
       "screen_name": row[6],
       "description": row[7],
       "friends_count": row[8],
       "created_at": row[9],
       "source": row[10],
       "in_reply_to_user_id": row[11],
       "in_reply_to_screen_name": row[12],
       "in_reply_to_status_id": row[13],
       "retweet_count": row[14],
       "contributors": row[15],
       "geo_type": row[16]
   }
   for row in fullTable
]

# Export the full table to a JSON file
with open('fullTable.json', 'w') as f:
   json.dump(fullTable_json, f, indent=4)
   
with open('fullTable.csv', 'w',  encoding='utf-8') as f:
   headers = [description[0] for description in cursor.description] # Extract column names from the cursor
   f.write(','.join(headers) + '\n') # Write column headers to the CSV file
   
   for i in fullTable:
       f.write(','.join(map(str, i)) + '\n') # Write each row of the table as a string separated by comma


conn.close()

# 3D

# Computing the size of each file
import os 
orginal = os.path.getsize("OneDayOfTweets_110.txt")
tweets_j = os.path.getsize("tweets.json")
tweets_c = os.path.getsize("tweets.csv")
table_j = os.path.getsize("fullTable.json")
table_c = os.path.getsize("fullTable.csv")



# Visualize file size comparison
import numpy as np
import matplotlib.pyplot as plt

data = {'Original file':orginal, 'Tweets JSON':tweets_j, 'Tweets CSV':tweets_c,
       'Table JSON':table_j, 'Table CSV':table_c} # Dictionary to hold file names and their sizes
courses = list(data.keys())
values = list(data.values())
 
fig = plt.figure(figsize = (20, 10))


plt.bar(courses, values, color ='maroon', 
       width = 0.4)

plt.xlabel("Files")
plt.ylabel("Size")
plt.title("File sizes")
plt.show()
