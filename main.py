from datetime import datetime,timedelta
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import time
import os
import sys  
import gzip
import shutil
import pandas as pd
import csv
import json
import ftplib
import constants as c

def unzip_gz_file(folder_path_in, folder_path_out, desired_file_name):
    """unzip a compressed .gz file
    
    Parameters
    ----------
    folder_path_in : str
        full path to the .gz file
    folder_path_out : str
        path to the desired destination folder
    desired_file_name : str
        the name that you want to the extracted file have, don't forget the file format
        
    Returns
    -------
    None
    """
    print(f'{time.ctime()}, Start of unzipping process')
    # create a list with the name of the files that exist in this folder
    file_name = os.listdir(folder_path_in)
    # ignore files that were created by the notebook execution
    thing = '.ipynb_checkpoints'
    if thing in file_name: file_name.remove(thing)
    # define the index of the most recent file
    last_file_index = len(file_name) - 1
    # create the full path to this file
    path_to_gz_file = os.path.join(folder_path_in, file_name[last_file_index])
    # define the full path to the unzipped file
    path_to_csv_file = os.path.join(folder_path_out, desired_file_name)
    print(f'{time.ctime()}, Folders defined')
    print(f'{time.ctime()}, Start unzipping file')
    # open the .gz file
    with gzip.open(path_to_gz_file, 'rb') as f_in:
        # create the uncompressed file on the desired path
        with open(path_to_csv_file, 'wb') as f_out:
            # file the file with the data inside the .gz
            shutil.copyfileobj(f_in, f_out)
    print(f'{time.ctime()}, File unzipped')
    
def upload_file_to_bucket(path_key_json, file_path, bucket_name, bucket_folder=None):
    """upload a file to a bucket on Cloud Storage at Google Cloud Platform, it could be at root or at a folder of the bucket
    
    Parameters
    ----------
    path_key_json :  str
        path to GCP service key, the key must be at .json format
    
    file_path : str
        full path to the desired file, example: 'C:\\Documents\\Data\\result\\test.csv'
    
    bucket_name : str
        name of the bucket that you want to ingest a file
    
    bucket_folder : str
        name of the folder of the bucket that you want to ingest a file
    
    Returns
    -------
    None
    """
    print(f"{time.ctime()}, Start upload file to Cload Storage Bucket process")
    # get the file name
    lst_file_path = file_path.split(f"\\")
    file_name = lst_file_path[len(lst_file_path)-1]
    # instantiates a client
    storage_client = storage.Client.from_service_account_json(path_key_json)
    print(f"{time.ctime()}, GCP storage client logon successful")
    # get bucket object
    bucket = storage_client.get_bucket(bucket_name)
    print(f"{time.ctime()}, Bucket object got")
    # define the blob path
    if bucket_folder:
        blob_path = bucket_folder + "/" + file_name
    else:
        blob_path = file_name
    # create the blob
    blob = bucket.blob(blob_path)
    print(f"{time.ctime()}, Bucket blob {blob_path} created")
    print(f"{time.ctime()}, Start uploading file {file_name}")
    # upload the files in GCS
    blob.upload_from_filename(file_path)
    print(f"{time.ctime()}, File {file_name} uploaded")
    
def read_file_from_ftp(dict_ftp, file_name_temp, file_format, destination_folder_path):
    """download a file from a FTP
    
    Parameters
    ----------
    dict_ftp : dict
        dictionary with the key to connect with the FTP
        {'host' : '', 'user' : '', 'password' : '', 'path' : ''}
    file_name_temp : str
        template of the desired file name file you want to download: order_parameters_export_2020-06-28_420348.csv, 
        template example: order_parameters_export_2020-06-28
    file_format : str
        Extension of the file, it could be '.csv', '.pdf', etc...
    destination_folder_path : str
        The folder where the 
        'C:\\Users\\Documents\\your_folder'
    Returns
    -------
    None
    """
    print(f'{time.ctime()}, Start download file from FTP process')
    download_ind = False
    # connect to FTP
    ftp = ftplib.FTP(dict_ftp['host']) 
    # log in to FTP
    ftp.login(dict_ftp['user'], dict_ftp['password'])
    print(f'{time.ctime()}, FTP connection stablished')
    # change directory in FTP
    ftp.cwd(dict_ftp['path'])
    print(f'{time.ctime()}, Access output directory')
    # for each file in the directory 
    for file in ftp.nlst():
        # check file name for file name template
        if file.find(file_name_temp) >= 0:
            print(f"{time.ctime()}, File found")
            while not download_ind:
                print(f"{time.ctime()}, Start downloading file")
                # define local file name
                localfile = os.path.join(destination_folder_path, file)
                localfile = open(localfile, 'wb')
                try:
                    ftp.retrbinary("RETR " + file, localfile.write)
                    download_ind = True
                    print(f"{time.ctime()}, File downloaded")
                except Exception as e:
                    print(f"{time.ctime()}, File could not be downloaded, an error has occurred")
                    print(f"{time.ctime()}, Error Message: {e}")
                    # delete what is inside the file
                    localfile.truncate(0)
                    # close file
                    localfile.close()
                    print(f"{time.ctime()}, File created to recieve data was deleted")
    # close FTP connection
    ftp.quit()
    print(f"{time.ctime()}, FTP connection closed")
    print(f'{time.ctime()}, End download file from FTP process')
    
def upload_file_to_bucket_sdk(file_path, dict_gc_auth, bucket_name, lst_bucket_folder=None):
    """upload a file to a bucket in Google Cloud Platform using Google Cloud Software Development Kit
    
    Parameters
    ----------
    file_path : str
        complete file path with directory, folder, file name and file format
    dict_gc_auth : dict
        dict that stores Google Cloud authorization data.
        {'email' : '', 'cred_json_file_path' : '', 'project_id' : '',}
    bucket_name : str
        name of the bucket
    lst_bucket_folder : list
        list with the names of the folders of the bucket
    
    Returns
    -------
    None
    """
    print(f'{time.ctime()}, Start of file upload process')
    # define the bucket path
    bucket_path = f'gs://{bucket_name}/'
    if len(lst_bucket_folder):
        for name in lst_bucket_folder:
            bucket_path = bucket_path + name + '/'
    try:
        print(f'{time.ctime()}, Start GCP loging')
        # # command line to run a cmd command on jupyter notebook
        # !gcloud auth activate-service-account {dict_gc_auth['email']} --key-file={dict_gc_auth['cred_json_file_path']}
        # try to logging in your google account on GCP using a GC SDK command line (works on .py files) 
        os.system(
            f"gcloud auth activate-service-account {dict_gc_auth['email']} --key-file={dict_gc_auth['cred_json_file_path']}")
        print(f'{time.ctime()}, GCP logged in')
    except Exception as e:
        print(f'{time.ctime()}, An error has occurred during the last step: {e}')
        sys.exit()
    try:
        print(f'{time.ctime()}, Start uploading file')
        # !gsutil -m cp {file_path} {bucket_path}
        # try to upload a file to a bucket in GCP using a GC SDK command line (works on .py files) 
        os.system(f'gsutil -m cp {file_path} {bucket_path}')
        print(f'{time.ctime()}, File uploaded')
    except Exception as e:
        print(f'{time.ctime()}, An error has occurred during the last step: {e}')
        sys.exit()
    print(f'{time.ctime()}, End of file upload process')
    
def clear_files_from_folders(directory_root, lst_folders):
    """clear all files that are inside of a list of folders of a directory
    
    Parameters
    ----------
    directory_root : str
        path to folder that has the folders that you want to clear inside of it
    lst_folders : list
        name of the folders that you want to clear
        
    Returns
    -------
    None
    """
    print(f'{time.ctime()}, Start cleaning process')
    # for each folder of the list
    for folder in lst_folders:
        print(f'{time.ctime()}, Folder defined')
        # create a list with all files inside this folder
        lst_files = os.listdir(os.path.join(directory_root, folder))
        for file in lst_files:
            try:
                # try to delete them
                os.remove(os.path.join(directory_root, folder, file))
            except:
                print(f'{time.ctime()}, It was not possible to delete {file}')
        print(f'{time.ctime()}, All files from folder were deleted')
    print(f'{time.ctime()}, All folders are clean')
    print(f'{time.ctime()}, End of cleaning process')
    
def execute():
    """upload a file from a FTP to a bucket on GCP
    
    Parameters
    ----------
    None
    
    Returns
    -------
    None
    """
    print(f'{time.ctime()}, Start of the process')
    # # set initial values
    # folders path variables
    directory_root = os.path.abspath("")
    folder_path_in = os.path.join(directory_root, c.lst_folders[0])
    folder_path_out = os.path.join(directory_root, c.lst_folders[1])
    # date variables
    dt_str = datetime.now().date().strftime("%Y-%m-%d")
    # variables to unzip file
    compressed_file_name = os.listdir(folder_path_in)[0]
    compressed_file_path = os.path.join(folder_path_in, compressed_file_name)
    desired_file_name = 'effectiveSalesForecast.csv'
    # clear every file
    clear_files_from_folders(directory_root, c.lst_folders)
    # download file from ftp
    read_file_from_ftp(
        dict_ftp = json.load(open(os.path.join(directory_root, 'key', 'key_ftp.json')))['relex_test'], 
        file_name_temp = c.dict_file_ftp['file_name_template'] + dt_str,
        file_format = c.dict_file_ftp['file_format'],
        destination_folder_path = folder_path_in)
    # unzip file
    unzip_gz_file(folder_path_in, folder_path_out, desired_file_name)
    # upload file
    upload_file_to_bucket_sdk(
        file_path = os.path.join(folder_path_out,desired_file_name),
        dict_gc_auth = c.dict_gc_auth,
        bucket_name = c.dict_bucket['name'],
        lst_bucket_folder = c.dict_bucket['lst_folder'])
    # clear every file
    clear_files_from_folders(directory_root, c.lst_folders)
    print(f'{time.ctime()}, End of the process')
    
if __name__ == "__main__":
    execute()