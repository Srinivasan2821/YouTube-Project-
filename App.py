api_key = "AIzaSyBQBbz2QTAE_aY6CacSyrd2GSJkyI3VVcg"

import googleapiclient.discovery
from pprint import pprint
import pymongo
import streamlit as st
import mysql.connector
import pandas as pd
import isodate

#from tabulate import tabulate   To See Data in Table From

api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

#ID = str(input("enter Channel ID:"))
#ID="UCS3c38DmZc_eA3wlp3uHqIw"
# Hari Baskar - UCS3c38DmZc_eA3wlp3uHqIw


#(ch_details(ID))
def ch_details(ID):

    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=ID)
    response = request.execute()


    ch = dict(
        channel_id=response['items'][0]['id'],
        channel_name=response['items'][0]['snippet']['title'],
        channel_publish=response['items'][0]['snippet']['publishedAt'],
        channel_description=response['items'][0]['snippet']['description'],
        channel_subscribercount=response['items'][0]['statistics']['subscriberCount'],
        channel_videos=response['items'][0]['statistics']['videoCount'],
        channel_views=response['items'][0]['statistics']['viewCount'],
        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    )
    return ch


# Playlist Detatils 
def playlist_details(ID):

    All_data = []
    next_page_token = None
    next_page = True

    while next_page:

        request = youtube.playlists().list(part="snippet,contentDetails",channelId=ID,maxResults=50,pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:

            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            
            All_data.append(data)

        next_page_token = response.get('nextPageToken')

        if next_page_token is None:
            next_page=False

    return All_data

#video IDS
def Video_IDs(ID):

    video_ids = []
    res = youtube.channels().list(id=ID,part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(part = 'snippet',playlistId = playlist_id,maxResults = 50,pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break

    return video_ids

#Video Details
def video_details(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id= video_id)
        response = request.execute()

        for item in response["items"]:

            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = int(pd.Timedelta(item['contentDetails']['duration']).total_seconds()),
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)

    return video_data

#Comment Details
def comment_details(video_ids):
        
        Comment_Data = []

        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(part = "snippet",videoId = video_id,maxResults = 20)
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_data = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Data.append(comment_data)
        except:
                pass
                
        return Comment_Data


# Connect MongoDB
from pymongo.mongo_client import MongoClient
client = MongoClient("mongodb+srv://srinivasan:Sowmi@cluster0.q8zuohu.mongodb.net/?retryWrites=true&w=majority")

db = client["Youtube_data"]

#SQL connection
mydb = mysql.connector.connect(host="localhost",
            user="root",
            password="",
            database= "youtube_datas",
            port = "3306"
            )
mycursor = mydb.cursor()

#Import Data To MongoDB Database
def Channel_Details(ID):
    Ch_details = ch_details(ID)
    Pl_details = playlist_details(ID)
    vi_id = Video_IDs(ID)
    Vi_details = video_details(vi_id)
    Com_details = comment_details(vi_id)
    #Channel_Name = input("Enter Channel Name:")
    collection1 = db["Channel_Datas"]
    collection1.insert_one({"Channel_Details":Ch_details,"Playlist_Details":Pl_details,"Video_Details":Vi_details,
                     "Comment_Details":Com_details})
    
    return "upload completed successfully"



# Channel Table
def channels_table():

    try:
        create_query = '''CREATE TABLE channels (channel_id VARCHAR (80) primary key,
                                                    channel_name VARCHAR (100),
                                                    channel_publish timestamp,
                                                    channel_description text,
                                                    channel_subscribercount int,
                                                    channel_videos int,
                                                    channel_views int, 
                                                    playlist_id VARCHAR (100))'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table already created")    


    ch_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for ch_data in collection1.find({"Channel_Details.channel_name":channel_names}):
        ch_list.append(ch_data["Channel_Details"])
    df = pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT INTO channels (channel_id, channel_name, channel_publish, 
                                                      channel_description, channel_subscribercount, 
                                                     channel_videos, channel_views, playlist_id) 
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
            

        values =(
                row['channel_id'],
                row['channel_name'],
                row['channel_publish'],
                row['channel_description'],
                row['channel_subscribercount'],
                row['channel_videos'],
                row['channel_views'],
                row['playlist_id'])
        try:                     
            mycursor.execute(insert_query,values)
            mydb.commit()    
        except:
            pass

    st.write("Channels values are inserted")

# Playlist Table
def playlists_table():

    try:
        create_query = '''CREATE TABLE playlists (PlaylistId VARCHAR (100) primary key,
                                                            Title VARCHAR (100),
                                                            ChannelId VARCHAR (100),
                                                            ChannelName VARCHAR (100),
                                                            PublishedAt timestamp,
                                                            VideoCount int)'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists Table already created")    

    pl_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for pl_data in collection1.find({"Channel_Details.channel_name":channel_names}):
        for i in range(len(pl_data["Playlist_Details"])):
                pl_list.append(pl_data["Playlist_Details"][i])
    df = pd.DataFrame(pl_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into playlists (PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
        values =(
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
                
        try:                     
            mycursor.execute(insert_query,values)
            mydb.commit()    
        except:
            pass

    st.write("Playlists values are inserted")

# Video Table
def videos_table():

    try:
        create_query = '''CREATE TABLE videos (Channel_Name VARCHAR(100),
                                                Channel_Id VARCHAR(100),
                                                Video_Id VARCHAR(100) PRIMARY KEY,
                                                Title VARCHAR(100),
                                                Tags TEXT,
                                                Thumbnail VARCHAR(255),
                                                Description TEXT,
                                                Published_Date TIMESTAMP,
                                                Duration INT,
                                                Views INT,
                                                Likes INT,
                                                Comments INT,
                                                Favorite_Count INT,
                                                Definition VARCHAR(100),
                                                Caption_Status VARCHAR(100))'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Video Table already created")    

    vi_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for vi_data in collection1.find({"Channel_Details.channel_name":channel_names}):
        for i in range(len(vi_data["Video_Details"])):
                vi_list.append(vi_data["Video_Details"][i])
    df = pd.DataFrame(vi_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT INTO videos (Channel_Name, Channel_Id, 
                                                Video_Id, Title,    
                                                Tags, Thumbnail, Description,
                                                Published_Date,
                                                Duration, Views, Likes,
                                                Comments, Favorite_Count,
                                                Definition, Caption_Status) 
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''            
        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status'])
                
        try:                     
            mycursor.execute(insert_query,values)
            mydb.commit()    
        except:
            pass

    st.write("Video values are inserted")

# Comments Table
def comments_table():

    try:
        create_query = '''CREATE TABLE comments (Comment_Id VARCHAR (100) primary key,
                                                    Video_Id VARCHAR (100),
                                                    Comment_Text text,
                                                    Comment_Author VARCHAR (100),
                                                    Comment_Published timestamp)'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Comment Table already created")  

    com_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for com_data in collection1.find({"Channel_Details.channel_name":channel_names}):
        for i in range(len(com_data["Comment_Details"])):
                com_list.append(com_data["Comment_Details"][i])
    df = pd.DataFrame(com_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, 
                                        Comment_Author, Comment_Published) 
                                        VALUES (%s, %s, %s, %s, %s)'''            
        values =(
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
                
        try:                     
            mycursor.execute(insert_query,values)
            mydb.commit()    
        except:
            pass
        
    st.write("Comments values are inserted")

# Creating Tables
def Tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"

# show channel table
def show_channel_table():
    ch_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for ch_data in collection1.find({"Channel_Details.channel_name":channel_names}):
        ch_list.append(ch_data["Channel_Details"])
    channels_table = st.dataframe(ch_list)
    return channels_table

# show playlist table
def show_playlist_table():
    pl_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for pl_data in collection1.find({"Playlist_Details.ChannelName":channel_names}):
        for i in range(len(pl_data["Playlist_Details"])):
                pl_list.append(pl_data["Playlist_Details"][i])
    playlist_table = st.dataframe(pl_list)
    return playlist_table

# show video table
def show_video_table():
    vi_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for vi_data in collection1.find({"Video_Details.Channel_Name":channel_names}):
        for i in range(len(vi_data["Video_Details"])):
                vi_list.append(vi_data["Video_Details"][i])
    video_table = st.dataframe(vi_list)
    return video_table

# show comment table
def show_comment_table():
    com_list = []
    db = client["Youtube_data"]
    collection1 = db["Channel_Datas"]
    for com_data in collection1.find({"Channel_Details.channel_name":channel_names}):
        for i in range(len(com_data["Comment_Details"])):
                com_list.append(com_data["Comment_Details"][i])
    comment_table = st.dataframe(com_list)
    return comment_table

#Seconds to Duration
def seconds_to_iso8601(average_duration):
    hours = average_duration // 3600
    minutes = (average_duration % 3600) // 60
    average_duration = average_duration % 60
    return f"PT{hours}H{minutes}M{average_duration}S"

#Streamlit
st.title(":rainbow[_YouTube Data Harvesting and Warehousing :red[: ▶]_]")

Menu=st.sidebar.selectbox(":red[_**Please Select The Menu:-**_]",("Home","Data Transfer","Migrate Date","Query"))

if Menu == "Home":
    with st.sidebar:
            st.header(":red[_Skill:-_]")
            st.write(':blue[ :star: Python scripting]') 
            st.write(':blue[ :star: API Integration]')
            st.write(':blue[ :star: Data Collection]')
            st.write(':blue[ :star: MongoDB]')
            st.write(':blue[ :star: Data Managment using MongoDB and SQL]')
            st.write(':blue[ :star: Streamlit]')
    st.header("_**Project**_")
    st.subheader(':gray[Set up a Streamlit app:-]') 
    st.write('Streamlit is a great choice for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.')
    st.subheader(':gray[Connect to the YouTube API:-]') 
    st.write("You'll need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.")
    st.subheader(':gray[Store data in a MongoDB data lake:-]') 
    st.write("Once you retrieve the data from the YouTube API, you can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.")
    st.subheader(':gray[Migrate data to a SQL data warehouse:-]') 
    st.write("After you've collected data for multiple channels, you can migrate it to a SQL data warehouse. You can use a SQL database such as MySQL or PostgreSQL for this.")
    st.subheader(':gray[Query the SQL data warehouse:-]') 
    st.write("You can use SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input. You can use a Python SQL library such as SQLAlchemy to interact with the SQL database.")
    st.subheader(':gray[Display data in the Streamlit app:-]') 
    st.write("Finally, you can display the retrieved data in the Streamlit app. You can use Streamlit's data visualization features to create charts and graphs to help users analyze the data.")


if Menu == "Data Transfer":
    ID = st.text_input("_Enter Channel ID_")
    channels = ID.split(',')
    channels = [ch.strip() for ch in channels if ch]

    if st.button("_Collect & Store_"):
        for channel in channels:
            ch_ids = []
            db = client["Youtube_data"]
            collection1 = db["Channel_Datas"]
            for ch_data in collection1.find({},{"_id":0,"Channel_Details":1}):
                ch_ids.append(ch_data["Channel_Details"]["channel_id"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = Channel_Details(channel)
                st.success(output)

if Menu == "Migrate Date":
    def mongo_connect():
        client = MongoClient("mongodb+srv://srinivasan:Sowmi@cluster0.q8zuohu.mongodb.net/?retryWrites=true&w=majority")
        db = client["Youtube_data"]
        collection1 = db["Channel_Datas"]
        return collection1
    def list_channel():
        coll=mongo_connect()
        list_channels=[i["Channel_Details"]["channel_name"] for i in coll.find({})]
        return list_channels 
    
    channel_names=st.selectbox("Select the Channel",list_channel())

    if st.button("_Migrate Data to SQL_"):
        display = Tables()
        st.success(display)

    show_table = st.sidebar.radio(":red[_**Select The Table For View:-**_]",(":blue[channels]",":blue[playlists]",":blue[videos]",":blue[comments]"))

    if show_table == ":blue[channels]":
        show_channel_table()
    elif show_table == ":blue[playlists]":
        show_playlist_table()
    elif show_table ==":blue[videos]":
        show_video_table()
    elif show_table == ":blue[comments]":
        show_comment_table()


if Menu == "Query":
    Questions=st.selectbox(
        ":red[_**Please Sellect:-**_]",
                            ('1. All Videos and Their Channel Name',
                            '2. Channels have the most number of videos and their Counts',
                            '3. Top 10 most viewed videos and their Channels',
                            '4. Number of Comments on Each Video  and their Names',
                            '5. Vidoes with Highest Likes and Their Channel Name',
                            '6. Number of Likes for Videos and Their Names',
                            '7. Number of Views for Channels',
                            '8. Channel Name those Published Video in 2022',
                            '9. Average Duration of All Videos in Each Channel with channel Name',
                            '10. Vidoes with Highest Number of Comments and Their Channel Names'))

    if Questions == '1. All Videos and Their Channel Name':
        query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
        mycursor.execute(query1)
        t1=mycursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))


    elif Questions == '2. Channels have the most number of videos and their Counts':
        query2 = "select channel_name as ChannelName,channel_videos as No_of_Videos from channels order by channel_videos desc;"
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))


    elif Questions == '3. Top 10 most viewed videos and their Channels':
        query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos
                            where Views is not null order by Views desc limit 10;'''
        mycursor.execute(query3)
        t3 = mycursor.fetchall()
        st.write(pd.DataFrame(t3, columns = ["Views","Channel Name","Video Title"]))

    elif Questions == '4. Number of Comments on Each Video  and their Names':
        query4 = '''select Comments as No_of_comments ,Title as VideoTitle from videos where Comments is not null;'''
        mycursor.execute(query4)
        t4 = mycursor.fetchall()
        st.write(pd.DataFrame(t4, columns = ["No of Comments","Video Title"]))

    elif Questions == '5. Vidoes with Highest Likes and Their Channel Name':
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                        where Likes is not null order by Likes desc;'''
        mycursor.execute(query5)
        t5 = mycursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Video Title","Channel Name","Like Count"]))

    elif Questions == '6. Number of Likes for Videos and Their Names':
        query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
        mycursor.execute(query6)
        t6 = mycursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Like Count","Video Title"]))

    elif Questions == '7. Number of Views for Channels':
        query7 = "select channel_name as ChannelName, channel_views as Channelviews from channels;"
        mycursor.execute(query7)
        t7=mycursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name","Total Views"]))

    elif Questions == '8. Channel Name those Published Video in 2022':
        query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                    where extract(year from Published_Date) = 2022;'''
        mycursor.execute(query8)
        t8=mycursor.fetchall()
        st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

    elif Questions == '9. Average Duration of All Videos in Each Channel with channel Name':
        query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
        mycursor.execute(query9)
        t9=mycursor.fetchall()
        t9 = pd.DataFrame(t9, columns=['ChannelName', 'average_duration'])
        T9=[]
        for index, row in t9.iterrows():
            channel_title = row['ChannelName']
            average_duration = row['average_duration']
            average_duration_str = seconds_to_iso8601(average_duration)
            T9.append({"ChannelName": channel_title ,  "average_duration": average_duration_str})
        st.write(pd.DataFrame(T9)) 


    elif Questions == '10. Vidoes with Highest Number of Comments and Their Channel Names':
        query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                        where Comments is not null order by Comments desc;'''
        mycursor.execute(query10)
        t10=mycursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))

