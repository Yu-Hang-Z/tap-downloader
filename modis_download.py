import os
import sys
import time
import json
import shutil
import logging
from datetime import datetime, timedelta
from urllib import parse
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MODIS_Aqua = 'Aqua'
MODIS_Terra = 'Terra'
MODIS_PRODUCTS = [MODIS_Aqua, MODIS_Terra]
USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')
MODIS_ROOT = '/public/Raw/'
MODIS_LOCAL_DIR = MODIS_ROOT + 'MODIS/%s/%s/'
MODIS_OSS = '/MODIS/%s/%s/'
LADSWEB = 'https://ladsweb.modaps.eosdis.nasa.gov'
MODIS_URL = '/archive/Science Domain/Atmosphere/Aerosol/MODIS %s C6.1 - Aerosol 5-Min L2 Swath 10km/%s/%s'
MODIS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6Inp5aHoiLCJleHAiOjE3MzI0MTQ0NjcsImlhdCI6MTcyNzIzMDQ2NywiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292In0.Z3soYaWXnDmPZN9Mw54Bo4FeHZzk3Wir7YIS4u9Ei5697ioxQlOgjKZwvYjSdtoLDGAp3pFPU_JdAetDXLvixCilgT8zi6Tp8WENQUNR-iMtfdEAO_c_IZH9Cyf3t6wgCh4qkccaR-qSu_Jc8o5vWrtBrEJEwij1NjFGsQODfsfBAgpM41eC-U7rjWooWFCoMpPkfP6lcR3L0__fkzN3ynMBZhyCrwoh_Hs34Zulha2diItvs4ZO9Lg1ROyjl5gBHlOo9uRirzHo2xy8jOxZ4IMkTvvrOoRyeN8oeT7GDam34CMstgZMy7q1e2UKt6uDCJ3iL1ErlDiA2Pq_1VdXOg'

# Retry mechanism
def create_session_with_retries(max_retries=5, backoff_factor=1):
    session = requests.Session()
    retries = Retry(total=max_retries, backoff_factor=backoff_factor,
                    status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def download_file(session, url, save_path, expected_size, max_retries=5, backoff_factor=1, token=None):
    retry_attempts = 0
    headers = {'user-agent': USERAGENT}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    while retry_attempts < max_retries:
        try:
            with session.get(url, headers=headers, stream=True, timeout=60) as response:
                if response.status_code == 200:
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    with open(save_path, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    actual_size = os.path.getsize(save_path)
                    if actual_size == expected_size:
                        logging.info(f'Downloaded {os.path.basename(save_path)} successfully')
                        return
                    else:
                        logging.error(f'Size mismatch for {os.path.basename(save_path)}: expected {expected_size}, got {actual_size}')
                        os.remove(save_path)
                        return
                else:
                    logging.error(f'Failed to download {os.path.basename(save_path)}: HTTP {response.status_code}')
                    return
        except requests.exceptions.RequestException as e:
            logging.error(f'Error downloading {os.path.basename(save_path)}: {e}')
            os.remove(save_path)
            retry_attempts += 1
            sleep_time = backoff_factor * (2 ** retry_attempts)
            logging.info(f'Retrying in {sleep_time} seconds...')
            time.sleep(sleep_time)
    logging.error(f'Max retries exceeded for {os.path.basename(save_path)}')

def download_files(product, start_date, end_date):
    session = create_session_with_retries()
    date = start_date
    with session, ThreadPoolExecutor(max_workers=5) as executor:
        while date <= end_date:
            curr_year = date.strftime("%Y")
            curr_date = date.strftime("%Y-%m-%d")
            base_url = LADSWEB + parse.quote(MODIS_URL % (product, curr_year, '%03d' % date.timetuple().tm_yday))
            dest_dir = MODIS_LOCAL_DIR % (product, curr_year)
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
            local_file = os.path.join(dest_dir, curr_date + '.json')

            # Get the file list for the current date
            files = profile(base_url, MODIS_TOKEN)
            if files is None:
                logging.error(f'No files found for {curr_date}')
                date += timedelta(days=1)
                continue

            tasks = []
            for f in files:
                file_name = f['name']
                file_size = int(f['size'])
                file_url = base_url + '/' + file_name
                file_path = os.path.join(dest_dir, file_name)
                # Skip if file already exists and has the correct size
                if not os.path.exists(file_path) or os.path.getsize(file_path) != file_size:
                    tasks.append(executor.submit(download_file, session, file_url, file_path, file_size, token=MODIS_TOKEN))
                else:
                    logging.info(f'Skipping {file_name} (already exists and size matches)')

            # Wait for all download tasks to complete
            for task in tasks:
                task.result()
            date += timedelta(days=1)

def profile(url, token):
    ''' Fetch the file list from the given URL '''
    headers = {'Authorization': 'Bearer ' + token}
    response = requests.get(url + '.json', headers=headers)
    if response.status_code == 200:
        file_json = response.json()
        file_list = file_json['content']
        return file_list
    return None

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python modis_download.py <start_date> <end_date>")
        sys.exit(1)

    start_str = sys.argv[1]
    end_str = sys.argv[2]
    start_date = datetime.strptime(start_str, '%Y%m%d')
    end_date = datetime.strptime(end_str, '%Y%m%d')

    for modis_type in MODIS_PRODUCTS:
        download_files(modis_type, start_date, end_date)
