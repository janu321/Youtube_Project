import pymongo as pmo
from googleapiclient.discovery import build
from pymongo import MongoClient
import psycopg2
import pandas as pd
import streamlit as st



# Set up MongoDB connection
connection = MongoClient('mongodb+srv://Mithra:SanndRtagh@cluster0.cntykjq.mongodb.net/?retryWrites=true&w=majority')  # Replace with your MongoDB connection string
db = connection['youtube_Project']  #Creating Data base
# db.youtube_data.drop()
youtube_collection = db['youtube_data']  # Collection to store all YouTube data

#setup sql connection
import sqlalchemy
engine = sqlalchemy.create_engine("postgresql://postgres:rootroot@database-1.cikxwfpol0xz.eu-north-1.rds.amazonaws.com:5432/postgres")
api_key = "AIzaSyA17KLIcOdNUydY2Uw7bPe8ZDF436bnDGg"
youtube = build('youtube', 'v3', developerKey=api_key)




def get_channel_details(channel_id):
    request = youtube.channels().list(part='snippet,contentDetails,statistics', id=channel_id)
    response = request.execute()
    # return response.get('items', [])
    d1 = {'channel_name': response['items'][0]['snippet']['title'],
        'channel_id': response['items'][0]['id'],
        'subscriber_count': int(response['items'][0]['statistics']['subscriberCount']),
        'channel_tot_views': int(response['items'][0]['statistics']['viewCount']),
        'playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
        'video_count': int(response['items'][0]['statistics']['videoCount']),
        'channel_description': response['items'][0]['snippet']['description']
                      }
    # print(d1)
    return d1

def get_playlists(channel_id):
    playlists = []
    p1=[]
    Playlist_request = youtube.playlists().list(part='snippet,contentDetails',maxResults=5, channelId=channel_id).execute()
    # print(Playlist_request)
    for playlist in Playlist_request['items']:
        playlist_id = playlist['id']
        pl = {'playlist_id': playlist_id,
              'channel_di':channel_id,
            'title': playlist['snippet']['title'],
            'description': playlist['snippet']['description'],
            'published_at': playlist['snippet']['publishedAt']}
        playlists.append(pl)
    return playlists

def get_videos(playlist_id):
    videos = []
    request = youtube.playlistItems().list(part='snippet',maxResults=50, playlistId=playlist_id)
    response = request.execute()
    videos.extend(response.get('items', []))
    return videos

def get_video_detail(video_id,playlist_id,channel_id):
  # videos_detail = []
  global v1
  request = youtube.videos().list(part='snippet,contentDetails,statistics',id=video_id)
  response = request.execute()
  # print('final_video_data',response)
  for video in response['items']:
      v1={'video_id': video_id,
          'video_name': video['snippet']['title'],
          'description': video['snippet']['description'],
          'published_date': video['snippet']['publishedAt'],
          'channel_id': channel_id,
          'view_count': int(video['statistics']['viewCount']),
          'like_count': int(video['statistics']['likeCount']),
          'duration': video['contentDetails']['duration'],
          'comment_count': int(video['statistics']['commentCount']),
          # 'playlist_id': video['snippet']['playlistId'] if 'playlistId' in video['snippet'] else None
           'playlist_id':playlist_id
                    }
  videos_detail=(v1)
  return videos_detail


def get_comments_detail(video_id):
    comments = []
    try:
        next_page_token = None
        while True:
            cmt_request = youtube.commentThreads().list(part='snippet', videoId=video_id, maxResults=50,pageToken=next_page_token).execute()
            for cmt in cmt_request['items']:
              c1={'Comment_id': cmt['id'],
                    'video_id': cmt['snippet']['videoId'],
                    'Comment_txt':cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'author_name':cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'like_count': int(cmt['snippet']['topLevelComment']['snippet']['likeCount']),
                    'comment_published_date':cmt['snippet']['topLevelComment']['snippet']['publishedAt']}
              comments.append(c1)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
              # print('comments',comments)
    except:
        pass
    return comments


def get_all_youtube_data(channel_id):
    # Fetch channel details
    channel_details = get_channel_details(channel_id)

    # Fetch playlists for the channel
    playlists = get_playlists(channel_id)

    # Collecting all videos and comments data
    all_video_data = []
    all_comm_data=[]
    for playlist in playlists:
        playlist_id = playlist['playlist_id']
        # print("print playlist",playlist_id)
        videos = get_videos(playlist_id)
        # vi=[]
        for video in videos:
            video_id = video['snippet']['resourceId']['videoId']
            # print(video_id)
            video_info=get_video_detail(video_id,playlist_id,channel_id)
            # print(video_info)
            comments = get_comments_detail(video_id)
            all_comm_data.append(comments)
            all_video_data.append(video_info)

    # Prepare data to insert into mongo db as a document
    data_to_insert = {
        'channel_details': channel_details,
        'playlists': playlists,
        'video_detail': all_video_data,
        'comments_details': all_comm_data
    }

    return data_to_insert
def load_mongo_data_into_sql():
    # Transform data to DataFrame
    sam = engine.connect()

    #  Transform  playlist info to DataFrame
    pl_df2 = pd.DataFrame()
    for x in youtube_collection.find({}, {"_id": 0}):
        mongo_data = (x['playlists'])
        df = pd.DataFrame(mongo_data)
        # pl_df2=pl_df2.append(df,ignore_index=True)
        pl_df2 = pd.concat([pl_df2, df], ignore_index=True)
    pl_df2.to_sql("Playlist", engine, if_exists="replace", index=False)

    # Transform  video details  to DataFrame
    vi_df2 = pd.DataFrame()
    for x in youtube_collection.find({}, {"_id": 0}):
        mongo_data_vi = (x['video_detail'])
        df_vi = pd.DataFrame(mongo_data_vi)
        # vi_df2=vi_df2.append(df_vi,ignore_index=True)
        vi_df2 = pd.concat([vi_df2, df_vi], ignore_index=True)
        # print(vi_df2)
    vi_df2.to_sql("Video", engine, if_exists="replace", index=False)

    # Transform  comment details  to DataFrame
    cm_df2 = pd.DataFrame()
    cm_dffinal = pd.DataFrame()
    for x in youtube_collection.find({}, {"_id": 0}):
        mongo_data_cm = (x['comments_details'])
        for i in mongo_data_cm:
            df_cm = pd.DataFrame(i)
            cm_df2 = pd.concat([cm_df2, df_cm], ignore_index=True)
    cm_dffinal = pd.concat([cm_dffinal, cm_df2], ignore_index=True)
    cm_dffinal.to_sql("Comments", engine, if_exists="replace", index=False)


    # Transform  channel details  to DataFrame
    ch_df2 = pd.DataFrame()
    for x in youtube_collection.find({}, {"_id": 0}):
        mongo_data_ch = (x['channel_details'])
        df_ch = pd.DataFrame([mongo_data_ch])
        ch_df2 = pd.concat([ch_df2, df_ch], ignore_index=True)
    ch_df2.to_sql("Channel", engine, if_exists="replace", index=False)
    st.write("The documents are successfully loaded into Sql database ")


# loop to execute all channel id
def main():
    channel_id_list=["UCWetQ9ZanDSdHQVKNIjKrsg","UC9pRPRlo6wIOakEOi_2RWwA","UC2J_VKrAzOEJuQvFFtj3KUw",
                     "UCCj956IF62FbT7Gouszaj9w","UCBnxEdpoZwstJqC1yZpOjRA",
                                                "UCd3SZzlH4rzgvrcxiCV5r8Q",
                     "UC1sAQpHqHy4SV6ZSu98xGmg","UCMSI1Ck1mJOaxxwJ0bzrYhQ"]
    for channel_id in channel_id_list:
        mongodata= get_all_youtube_data(channel_id)
        # print(mongodata)
        youtube_collection.insert_one(mongodata) # Insert data into MongoDB
    # print("Youtube data is retrieved")
    st.write("Youtube data is retrieved ")

def user_input_channel_id(ch_id):
    channel_id = ch_id
    print("user input", channel_id)
    mongodata_single= get_all_youtube_data(channel_id)
    youtube_collection.insert_one(mongodata_single)# Insert data into MongoDB
    ext = youtube_collection.find_one({"channel_details.channel_id": channel_id}, {"_id": 0})
    return ext

def fetch_given_channel_Details(channel_id1):
    print(channel_id1)
    x = youtube_collection.find_one({"channel_details.channel_id": channel_id1}, {"_id": 0})
    return x

def display_channel_info(selected_option):
    channel_id =""
    st.write(f"You selected: {selected_option}")
    if selected_option == "MG x PODCAST":
        channel_id= "UC9pRPRlo6wIOakEOi_2RWwA"
    elif selected_option == "Chennai Super Kings":
        channel_id= "UC2J_VKrAzOEJuQvFFtj3KUw"
    elif selected_option == "BBC":
        channel_id = "UCCj956IF62FbT7Gouszaj9w"
    elif selected_option == "Sun TV":
        channel_id = "UCBnxEdpoZwstJqC1yZpOjRA"
    elif selected_option == "VB Dace Store":
        channel_id = "UCWetQ9ZanDSdHQVKNIjKrsg"
    elif selected_option == "The Urban Fight":
        channel_id = "UCMSI1Ck1mJOaxxwJ0bzrYhQ"
    elif selected_option == "Moviebuff Malayalam":
        channel_id = "UCd3SZzlH4rzgvrcxiCV5r8Q"
    elif selected_option == "Times Of India":
        channel_id = "UC1sAQpHqHy4SV6ZSu98xGmg"
    # elif selected_option == "Error Makes Clever Academy":
    #     channel_id = "UCwr-evhuzGZgDFrq_1pLt_A"

    return channel_id


# icon = Image.open("Youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   layout= "wide",
                   initial_sidebar_state= "expanded")
st.markdown("#    ")
st.write("#### :blue[Enter YouTube Channel_ID below :]")
ch_id = (st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id"))
if ch_id and st.button("Extract Data"):
   ch_details = user_input_channel_id(ch_id)
   st.write(f'The data for the given channel Id is extracted and loaded into Mongodb',ch_details)
   # st.table(ch_details)

st.write('#### :blue[Please click the below button to retrieve the youtube data for other channels]')
if st.button('Get youtube data '):
    main()

st.write('#### :blue[Choose any channel from the below list and get their channel information ]\n')

options = ['MG x PODCAST', 'Chennai Super Kings', 'BBC','Sun TV','VB Dace Store','The Urban Fight',
           'Moviebuff Malayalam','Times Of India','Dr Pal']
selected_option = st.selectbox(f'## :blue[Channel Names]',options,placeholder="Choose an option")

if st.button('Execute'):
    channel_id1 = display_channel_info(selected_option)
    st.write(f"The channel id for the selected channel is :",channel_id1)

    fav_channel=fetch_given_channel_Details(channel_id1)
    st.write(f"Please find the channel details :\n",fav_channel)

st.write("\n\n")
st.write('#### :blue[Please click the below button to extract the doc from datalake(Mongo db) and load into Postgres sql ]')
if st.button('Click here'):
    load_mongo_data_into_sql()


connection = psycopg2.connect(database="postgres",
                        host="database-1.cikxwfpol0xz.eu-north-1.rds.amazonaws.com",
                        user="postgres",
                        password="rootroot",
                        port="5432")


st.write("#### :blue[Select any one question to get Insights about Youtube channel]")
questions = st.selectbox('Questions',
   ['Click the question that you would like to query',
   '1. What are the names of all the videos and their corresponding channels?',
   '2. Which channels have the most number of videos, and how many videos do they have?',
   '3. What are the top 10 most viewed videos and their respective channels?',
   '4. How many comments were made on each video, and what are their corresponding video names?',
   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
   '8. What are the names of all the channels that have published videos in the year 2022?',
   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
  '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

if questions == '1. What are the names of all the videos and their corresponding channels?':
    query=""" select v.video_name,c.channel_name from public."Video" v left join public."Channel" c 
                    on v."channel_id"=c."channel_id" """
    df = pd.read_sql_query(query, connection)
    st.write(df)


elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    query = """ select c.channel_name as Channel_name,c.video_count as Total_Videos from public."Channel" c 
                order by Video_count desc """
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    query = """ select c.channel_name,v.video_name as video_title, v.view_count as total_views from public."Video" v 
                left join public."Channel" c on v."channel_id"=c."channel_id"
                order by v.view_count DESC limit 10 """
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    query = """ select video_name as video_title,comment_count as Total_comment_count from public."Video" """
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query = """ select v.video_name as video_title,c.channel_name,v.like_count as total_no_of_likes from public."Video" v 
                left join public."Channel" c on v."channel_id"=c."channel_id" order by v.like_count desc limit 10"""
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query = """ select video_name as video_title , like_count as total_number_of_likes from public."Video"
            order by like_count desc"""
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query = """ SELECT channel_name AS Channel_Name, Channel_Tot_Views AS Total_no_of_views FROM public."Channel"
                ORDER BY Channel_Tot_Views DESC"""
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    query = """ select c.channel_name from public."Video" v 
                left join public."Channel" c on v."channel_id"=c."channel_id"
                WHERE published_date LIKE '2022%'
                GROUP BY channel_name
                ORDER BY channel_name"""
    df = pd.read_sql_query(query, connection)
    st.write(df)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query = """ SELECT c.channel_name AS Channel_Name,  AVG(EXTRACT(EPOCH FROM 
           (SUBSTRING(v.duration FROM 'PT([0-9]+)M')::INTERVAL 
             + SUBSTRING(v.duration FROM 'PT[0-9]+M([0-9]+)S')::INTERVAL))) AS Average_Duration_Seconds
            FROM public."Video" v 
            left join public."Channel" c 
            on v."channel_id"=c."channel_id"
            GROUP BY c.channel_name"""
    df = pd.read_sql_query(query, connection)
    st.write(df)



elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query = """ select c.channel_name, v.video_name, v.video_id,v.comment_count as Total_Comment_Count from public."Video" v 
            left join public."Channel" c 
            on v."channel_id"=c."channel_id"
            order by comment_count DESC LIMIT 10"""
    df = pd.read_sql_query(query, connection)
    st.write(df)

