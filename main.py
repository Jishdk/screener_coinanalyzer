from bs4 import BeautifulSoup
import requests
import telegram
import asyncio
from google.cloud import storage
import json
import datetime
import re

# For TESTING only
#import os
#os.environ["GCLOUD_PROJECT"] = "coin-bot-project"

# The google cloud platform bucket name - change it according to yours
BUCKET_NAME = 'YOUR_BUCKET_NAME_HERE'

# The API key of the bot
API_KEY = "YOUR_TELEGRAM_API_KEY_HERE"

# Your chat id - you must start a conversation with the bot first
# The bot link is https://t.me/YOUR_BOT_LINK_HERE
# To get your chat id use https://t.me/userinfobot
# To get the group chat just enter this URL with your API key
# The ID's are integers!
GROUP_CHAT_ID = "YOUR_GROUP_CHAT_ID"
PRIVATE_CHAT_ID = "YOUR_PRIVATE_CHAT_ID"

# The url to the coinanalyze table
URL = "https://coinalyze.net/?filter=bl9ndF8zMDAwMDAwMA&columns=YiZuJmMmZCZlJnMmZyZoJnImaiZxJmwmbQ&order_by=oi_24h_pchange&order_dir=desc"

# Cache filename
CACHE_FILENAME = "coinanalyzer-cache.txt"

# Create the telegram bot and render the html page
bot = telegram.Bot(token=API_KEY)

async def log(message,chat_id):
    """
        The function logs a message to telegram bot and prints the same message to screen

    Args:
        message (<string>): The message to send to bot and output to screen
        chat_id (<int>): The chat id where to send the message
    """
    print(message)
    await bot.send_message(chat_id=chat_id, text=message)

def get_table_column_index_by_title(rows,title):
    """
        The function searches for the requested column in the table rows

    Args:
        rows (<htmlRows>): The bs4 tds
        title (<string>): The title to search for

    Raises:
        Exception: If no column found

    Returns:
        <int> : The index of the found column
    """
    span = rows[0].find("span",{'title': title})

    index = 0
    for child in rows[0].find_all("th"):
        if(child == span.parent.parent):
            return index
        index += 1
    raise Exception("No {} column".format(title))

def get_coin_column_index(rows):
    """
        The function searches for the coin name column in the table rows

    Args:
        rows (<htmlRows>): The bs4 tds

    Raises:
        Exception: If no column found

    Returns:
        <int> : The index of the found column
    """
    index = 0
    for child in rows[0].find_all("th"):
        if(child.contents == ["Coin"]):
            return index
        index += 1
    raise Exception("No coin column")

def upload_blob(blob_text, bucket_name=BUCKET_NAME, destination_blob_name=CACHE_FILENAME):
    """
        The function uploads a string to a gsp blob

    Args:
        blob_text (<string>): The string to upload
        bucket_name (<string>, optional): The name of the bucket. Defaults to BUCKET_NAME.
        destination_blob_name (<string>, optional): The name of the file to save to. Defaults to CACHE_FILENAME.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(blob_text)

def read_blob(bucket_name=BUCKET_NAME, destination_blob_name=CACHE_FILENAME):
    """
        The function reads a string from a gsp blob

    Args:
        bucket_name (<string>, optional): The name of the bucket. Defaults to BUCKET_NAME.
        destination_blob_name (<string>, optional): The name of the file to save to. Defaults to CACHE_FILENAME.

    Returns:
        <string>: The content of the blob
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(destination_blob_name)

    # If no blob exists or it was modified in a day other today (expired)
    if(not blob or blob.updated.date() < datetime.date.today() ):
        return "{}"
    else:
        return blob.download_as_string()

async def find_coin_anomaly(rows, potential_rise_coins_history):
    oi_index = get_table_column_index_by_title(rows,"Open Interest Change % 24H")
    oi_4h_index = get_table_column_index_by_title(rows,"Open Interest Change % 4H")

    coin_index = get_coin_column_index(rows)

    potential_rise_coins_current = {}

    # Loop through all columns but first
    for row in rows[1:]:
        # Extract coin names
        coin_names = row.find_all("td")[coin_index].find_all("span")
        long_coin_name = coin_names[0].contents[0]
        short_coin_name = coin_names[1].contents[0]

        # Get open interest info
        oi_chg = row.find_all("td")[oi_index].contents[0]        

        # The symbol (+/-) is the first char
        oi_chg_symbol = oi_chg[0]

        # The open interest value is between the first and last char (+/- and %)
        clean_float = re.findall(r'\d+\.\d+', oi_chg)[0]
        oi_chg_value = float(clean_float) if oi_chg[-2] != 'k' else float(clean_float)*1000
        oi_chg_value = oi_chg_value if oi_chg_symbol=="+" else -oi_chg_value

        if(oi_chg_value > 200):
            message = "{} {}".format(short_coin_name, oi_chg)
            await log(message,PRIVATE_CHAT_ID)

        elif(oi_chg_value > 70 or oi_chg_value < -50):
            message = "{} {}".format(short_coin_name, oi_chg)
            await log(message,GROUP_CHAT_ID)


        ####### 4H Open intrest Part #######
        
        # Get open interest info
        oi_4h_chg = row.find_all("td")[oi_4h_index].contents[0]        

        # The symbol (+/-) is the first char
        oi_4h_chg_symbol = oi_4h_chg[0]

        # The open interest value is between the first and last char (+/- and %)
        clean_float = re.findall(r'\d+\.\d+', oi_4h_chg)[0]
        oi_4h_chg_value = float(clean_float) if oi_4h_chg[-2] != 'k' else float(clean_float)*1000
        oi_4h_chg_value = oi_4h_chg_value if oi_4h_chg_symbol=="+" else -oi_4h_chg_value


        if(short_coin_name in potential_rise_coins_history.keys()):
            old_value = potential_rise_coins_history[short_coin_name]
            if(oi_4h_chg_value - old_value >= 30):
                message = "{} - changed from {} to {}".format(short_coin_name, old_value, oi_4h_chg_value)
                await log(message,PRIVATE_CHAT_ID)
            elif(oi_4h_chg < 1):
                potential_rise_coins_current[short_coin_name] = min(oi_4h_chg_value,old_value)

        elif(oi_4h_chg_value < 1):
            potential_rise_coins_current[short_coin_name] = oi_4h_chg_value

        
    # Update the history blob
    return potential_rise_coins_current

async def find_anomalies():
    # historical data from gsp blob
    potential_rise_coins_history = json.loads(read_blob())
    potential_rise_coins_current = {}
    page_idx = 1
    while True:
        current_url = "{}&p={}".format(URL,page_idx)
        page = requests.get(current_url)
        soup = BeautifulSoup(page.content, "html.parser")
        table = soup.find("table")
        rows = table.find_all("tr")
        if(len(rows) > 1):
            new_potential_coins = (await find_coin_anomaly(rows,potential_rise_coins_history))
            potential_rise_coins_current.update(new_potential_coins)
            page_idx += 1
        else:
            break
    upload_blob(json.dumps(potential_rise_coins_current))
    

def main(event, context):
    try:
        asyncio.run(find_anomalies())
    except Exception as e:
        asyncio.run(log("Oops, script raised following error:\n" + str(e),PRIVATE_CHAT_ID))

if __name__ == "__main__":
    main("", "")