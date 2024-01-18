from ftplib import FTP
import pandas as pd
from datetime import datetime
from ftplib import FTP
import os
import boto3
import pg8000
import time
import pysftp
import paramiko

import requests
import base64

def ftp_file_tree(ftp_host, ftp_user, ftp_passwd):
    # Connect to the FTP server
    ftp = FTP(ftp_host)
    ftp.login(user=ftp_user, passwd=ftp_passwd)
    
    # Set the working directory to the root directory
    ftp.cwd('/')

    # Initialize an empty list to store file information
    file_info_list = []

    # Function to recursively traverse the directory tree
    def traverse_ftp_directory(ftp, path=""):
        ftp.cwd(path)
        items = ftp.nlst()

        for item in items:
            try:
                # Check if it's a file or directory
                item_type = "File" if "." in item else "Directory"

                # Initialize modified_at and file_size as None
                modified_at = None
                file_size = None

                # Get file modification timestamp (if it's a file)
                if item_type == "File":
                    modified_at = datetime.strptime(ftp.sendcmd(f"MDTM {item}")[4:], "%Y%m%d%H%M%S")
                    file_size = ftp.size(item)

                # Append file information to the list
                file_info_list.append({
                    "path": path,
                    "filename": item,
                    "filesize": file_size,
                    "modified_at": modified_at
                })

                # Recursively process subdirectories (if it's a directory)
                if item_type == "Directory":
                    traverse_ftp_directory(ftp, f"{path}/{item}")
            except Exception as e:
                print(f"Error processing {item}: {e}")

    # Start the traversal from the root directory
    traverse_ftp_directory(ftp)

    # Close the FTP connection
    ftp.quit()

    # Create a DataFrame from the list of file information
    df = pd.DataFrame(file_info_list)

    return df


import io

def ftp_get_csv_to_dataframe(ftp_host, ftp_user, ftp_passwd, csv_path):
    try:
        # Connect to the FTP server
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_user, passwd=ftp_passwd)

        # Download the CSV file as bytes
        with io.BytesIO() as byte_buffer:
            ftp.retrbinary(f"RETR {csv_path}", byte_buffer.write)
            byte_buffer.seek(0)

            # Load the CSV data into a DataFrame
            df = pd.read_csv(byte_buffer)

        # Close the FTP connection
        ftp.quit()

        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

    
    
    
    
    
def sftp_file_tree(sftp_host, sftp_user, sftp_passwd):
    try:
        # Create an SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_user, password=sftp_passwd)

        # Create an SFTP client
        sftp = ssh.open_sftp()

        # Set the working directory to the root directory
        sftp.chdir('/')

        # Initialize an empty list to store file information
        file_info_list = []

        # Function to recursively traverse the directory tree
        def traverse_sftp_directory(sftp, path=""):
            items = sftp.listdir(path)

            for item in items:
                try:
                    # Check if it's a file or directory
                    item_type = "File" if "." in item else "Directory"

                    # Initialize modified_at and file_size as None
                    modified_at = None
                    file_size = None

                    # Get file modification timestamp (if it's a file)
                    if item_type == "File":
                        attrs = sftp.lstat(f"{path}/{item}")
                        modified_at = datetime.fromtimestamp(attrs.st_mtime)
                        file_size = attrs.st_size

                    # Append file information to the list
                    file_info_list.append({
                        "path": path,
                        "filename": item,
                        "filesize": file_size,
                        "modified_at": modified_at
                    })

                    # Recursively process subdirectories (if it's a directory)
                    if item_type == "Directory":
                        traverse_sftp_directory(sftp, f"{path}/{item}")
                except Exception as e:
                    print(f"Error processing {item}: {e}")

        # Start the traversal from the root directory
        traverse_sftp_directory(sftp)

        # Close the SFTP and SSH connections
        sftp.close()
        ssh.close()

        # Create a DataFrame from the list of file information
        df = pd.DataFrame(file_info_list)

        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

def sftp_get_csv_to_dataframe(sftp_host, sftp_user, sftp_passwd, csv_path):
    try:
        # Create an SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(sftp_host, username=sftp_user, password=sftp_passwd)

        # Create an SFTP client
        sftp = ssh.open_sftp()

        # Download the CSV file as bytes
        with io.BytesIO() as byte_buffer:
            sftp.get(csv_path, byte_buffer)

            byte_buffer.seek(0)

            # Load the CSV data into a DataFrame
            df = pd.read_csv(byte_buffer)

        # Close the SFTP and SSH connections
        sftp.close()
        ssh.close()

        return df
    except Exception as e:
        print(f"Error: {e}")
        return None
    
    
    
    
    
    
    
    
    
    
    
def connect_s3():

    session = boto3.Session(
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    return session.resource('s3')


def exportCSV(data, filepath):

    if os.path.exists(filepath):
        os.remove(filepath)
    data.to_csv(filepath, index=False, encoding='utf-8-sig')


def uploadS3(data, filepath, aws_path):

    exportCSV(data, filepath)
    s3 = connect_s3()
    s3.meta.client.upload_file(
        Filename=filepath,
        Bucket=os.getenv('AWS_BUCKET_NAME'),
        Key=aws_path
    )


def getS3Query(table, s3_path):

    s3query = f"copy {table} from " \
        + f"'s3://{os.getenv('AWS_BUCKET_NAME')}/{s3_path}' " \
        + "CREDENTIALS '" \
        + f"aws_access_key_id={os.getenv('AWS_ACCESS_KEY_ID')};" \
        + f"aws_secret_access_key={os.getenv('AWS_SECRET_ACCESS_KEY')}'" \
        + "csv IGNOREHEADER 1 TRUNCATECOLUMNS ACCEPTINVCHARS maxerror as 2048"

    return s3query


def upload_to_rdb(data, table_name, local_path=None, s3_path=None):

    if local_path == None:
        local_path = '../data/dump.csv'

    if s3_path == None:
        s3_path = 'bussines/dump.csv'

    uploadS3(data, local_path, s3_path)
    execute_sql(getS3Query(table_name, s3_path))


def connect_to_rdb():

    global cursor
    global con

    try:
        con = pg8000.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            port=os.getenv('DB_PORT')
        )
        
        con.autocommit = True
        cursor = con.cursor()

    except pg8000.InterfaceError:
        raise Exception('Cannot connect to RDB')


def disconnect_from_rdb():

    cursor.close()
    con.close()


def execute_sql(query):

    for _ in range(5):
        try:
            cursor.execute(query)
            success = True
            break

        except:
            refresh_connection()
            success = False

    assert success, 'Could not execute...'


def query_sql(query):

    for _ in range(5):
        try:
            data = pd.DataFrame(
                cursor.execute(query).fetchall(),
                columns = [desc[0] for desc in cursor.description]
            )
            success = True
            break

        except:
            refresh_connection()
            success = False

    assert success, 'Could not query...'

    return data


def refresh_connection():

    print('Refreshing Connection.')
    disconnect_from_rdb()
    time.sleep(5)
    connect_to_rdb()
    
    
    
def spotify_create_spotify_playlist(access_token, user_id, playlist_name, playlist_description):
    url = f"https://api.spotify.com/v1/users/{user_id}/playlists"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    data = {
        "name": playlist_name,
        "description": playlist_description,
        "public": True  # Set to True if you want the playlist to be public
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 201:
        playlist_id = response.json()['id']
        return playlist_id
    else:
        print("Failed to create playlist.")
        print(response.status_code, response.json())
        return None
    

def spotify_update_playlist(playlist_id,playlist_name,playlist_description):
    url = f"https://api.spotify.com/v1/playlists/"+playlist_id

    headers = {
        "Authorization": f"Bearer {spotify_bearer_token}",
        'Content-Type': 'application/json',
    }

    json_data = {
        'name': playlist_name,
        'description': playlist_description,
        'public': True,
    }
    response = requests.put(url, headers=headers, json=json_data)
    print(response.text)
    
    
def spotify_update_playlist_description(playlist_id,playlist_description):
    url = f"https://api.spotify.com/v1/playlists/"+playlist_id

    headers = {
        "Authorization": f"Bearer {spotify_bearer_token}",
        'Content-Type': 'application/json',
    }

    json_data = {
        'description': playlist_description,
        'public': True,
    }
    response = requests.put(url, headers=headers, json=json_data)
    print(response.text)
    
def spotify_upload_cover_img(access_token,playlist_id,cover_image_file):
    
    with open(cover_image_file, "rb") as f:
        cover_image_base64 = base64.b64encode(f.read()).decode("utf-8")

    # Update the playlist image using the Spotify Web API
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/images"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "image/jpeg"
    }
    data = cover_image_base64

    response = requests.put(url, headers=headers, data=data)

    if response.status_code == 202:
        print("Playlist cover image updated successfully!")
    else:
        print("Failed to update the playlist cover image.")
        print(response.status_code, response.json())
        
        
def spotify_add_tracks(playlist_id,uris):
    url = f"https://api.spotify.com/v1/playlists/"+playlist_id+"/tracks"

    headers = {
        "Authorization": f"Bearer {spotify_bearer_token}",
        'Content-Type': 'application/json',
    }
    json_data = {
        'uris': uris,
        'position': 0,
    }

    response = requests.post('https://api.spotify.com/v1/playlists/'+playlist_id+'/tracks', headers=headers, json=json_data)
    #print(response.text)
    
def get_bearer_token(client_id, client_secret, refresh_token):
    # Spotify Accounts service endpoint
    token_url = "https://accounts.spotify.com/api/token"

    # Encode the client_id and client_secret in Base64
    credentials = f"{client_id}:{client_secret}"
    credentials_b64 = base64.b64encode(credentials.encode()).decode('utf-8')

    # Set up the headers for the request
    headers = {
        'Authorization': f'Basic {credentials_b64}',
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    # Set up the data for the request
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    }

    # Make the request to obtain the access token
    response = requests.post(token_url, headers=headers, data=data)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the response JSON to get the access token
        access_token = response.json()['access_token']
        return access_token
    else:
        # Print an error message if the request fails
        print(f"Error: {response.status_code}")
        print(response.text)
        return None
    
def refresh_spotify_conn():
    global spotify_bearer_token
    
    spotify_bearer_token = get_bearer_token(os.getenv('SPOTIFY_CLIENT_ID'), os.getenv('SPOTIFY_CLIENT_SECRET_ID'), os.getenv('SPOTIFY_REFRESH_TOKEN_PLAYLISTS'))
    
    
    
def spotify_get_playlist_tracks(playlist_id):
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    headers = {
        'Authorization': f'Bearer {spotify_bearer_token}',
    }

    response = requests.get(playlist_url, headers=headers)

    if response.status_code == 200:
        return response.json()['items']
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

def spotify_remove_tracks_from_playlist(playlist_id,track_uris):
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    headers = {
        'Authorization': f'Bearer {spotify_bearer_token}',
        'Content-Type': 'application/json',
    }

    # Create a request payload
    data = {
        'tracks': [{'uri': uri} for uri in track_uris],
    }

    # Remove all tracks from the playlist
    response = requests.delete(playlist_url, headers=headers, json=data)

    if response.status_code == 200:
        print("All tracks removed from the playlist.")
        return True
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return False
    
    
def spotify_remove_all_tracks_from_playlist(playlist_id):
    
    
    while(True):
        # Get all tracks in the playlist
        tracks = spotify_get_playlist_tracks(playlist_id)

        if not tracks:
            break

        # Extract the track URIs
        track_uris = [track['track']['uri'] for track in tracks]

        if len(track_uris)==0:
            break

        spotify_remove_tracks_from_playlist(playlist_id,track_uris)
    