import os
from logger import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
from datetime import datetime
import time

load_dotenv()

def data_ingestion(interval_mins:5, num_runs:12, entries:int, DB_name:str, COLL_name:str):
    """
    This function encapsulates the function 'insert_into_Mongo_DB, that fectches API data from the 
    cryptocurrency website, COIN MARKET CAP, at regular intervals of time as
    given by the user, and inserts the data into a MongoDB collection for further 
    processing.
    The data fetched from the API is of JSON format and it is loaded into the 
    Database as documents.

    Args:
        interval_mins (int): Interval at which the data from the WebAPI is to fetched.
        num_runs (int): Number of times the data has to be fetched and loaded into the Database.
        entries (int): Number of entries that is to be fetched from the WebAPI at each call.
        DB_name (str): Name of the Database in MongoDB to be created, specified by the user.
        COLL_name (str): Name of the Collection in MongoDB to be created, under 'DB_name'.

    Returns:
        collection (pymongo.collection.Collection): Collection object of the MongoDB Database.
    """

    def insert_into_MongoDB(db_name:str, collection_name:str, entries:int):
        """
        This function connects to a MongoDB Database to inserts documents into the collection.
        It encapsulates functions 'api_runner' and 'connect_to_MongoDB'.

        Args:
            db_name (str): Name of the Database to be created/connected.
            collection_name (str): Name of the collection to be created/to connect, where each document is to be inserted.
            entries (int): Number of documents to be fetched from the WebAPI, and inserted into the MongoDB collection.

        Returns:
            collection (pymongo.collection.Collection): Collection object of the MongoDB Database.
        """ 
        def api_runner(req_num=5):
            """
            This function gets cryptocurrency data from the CoinMarketCap WebAPI.

            Args:
            req_num (int): integer value specifying the number of entries to be fetched at a time from the API.

            Returns:
            data (dict): json formatted dictionary that contains fetched data from the WebAPI.
            """ 

            url = os.environ["CMC_URL"]
            parameters = {
            'start':'1',
            'limit':f'{req_num}',
            'convert':'USD'
            }
            headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': os.environ["CMC_API_KEY"],
            }

            session = Session()
            session.headers.update(headers)

            try:
                response = session.get(url, params=parameters)
                data = json.loads(response.text)    
                return data
            except (ConnectionError, Timeout, TooManyRedirects) as e:
                print(e)

        def connect_to_MongoDB(db_name:str, collection_name:str):
            """
            This function connects to a MongoDB Database to inserts data.

            Args:
            db_name (str): Name of the database to be created/to connect.
            collection_name (str): Name of the collection to be created/to connect, where each document is to be inserted.

            Returns:
            collection (pymongo.collection.Collection): Collection object that is connected to the MongoDB Database.
            """ 

            client = MongoClient(os.environ["MONGODB_URI"])
            my_db=client[f'{db_name}']
            collection=my_db[f'{collection_name}']
            
            return collection

        data = api_runner(entries)
        ti=datetime.now().strftime("%H:%M:%S")
        logging.info(f"Fetched data from API at {ti}")
        t=datetime.now().strftime(f"%Y-%m-%dT%H:%M:%S")
        d=data["data"]
        for element in d:
            element['datetime_fetched']=t
        collection = connect_to_MongoDB(db_name, collection_name)
        collection.insert_many(d)
        ti=datetime.now().strftime("%H:%M:%S")
        logging.info(f"Inserted {len(d)} entries at time {ti}. Total documents in collection {collection_name} under Database {db_name} is {collection.count_documents({})}.")
        return collection

    n=0
    while (n<=num_runs):
        collection = insert_into_MongoDB(db_name=DB_name, collection_name=COLL_name, entries=entries)
        if (num_runs!=0):         
            time.sleep(interval_mins*60)
        n+=1
    ti=datetime.now().strftime("%H:%M:%S")
    logging.info(f"Returned collection after ingestion at {ti}")
    return collection


class aggregations:
    def __init__(self,collection):
        """
        Initializes the instance by defining the collection object and
        sets the output path as an instance variable, in the same directory 
        as the main.py file.
        """
        output_fname=f"output_agg.txt"
        self.output_path=os.path.join(os.getcwd(), output_fname)
        self.collection=collection 

    def write_to_output(self,df:pd.core.frame.DataFrame,title:str):
        """
        This function writes a dataframe into a text file as output.

        Args:
            df (pandas.dataframe.DataFrame): Dataframe (Pandas) to be written as output.
            output_path (str): Path of the output file to be stored.
            title (str): Title of the Dataframe to be included in the output.
        """
    
        f=open(self.output_path,"a")
        s=f"\t{title}\n"
        f.write(s)
        f.write("-"*80)
        f.write("\n")
        df_string = df.to_string(header=True, index=True, justify='center', float_format= lambda x: '{:,.2f}'.format(x))
        f.write(f"{df_string}\n\n\n\n")
        f.close()

    def agg_avg(self, title:str):
        """
        This function calculates the average price, volume and market cap of cryptocurrencies,
        and writes the dataframe into a text file as output.

        Args:
            title (str): Title of the Dataframe to be included in the output.
        """

        group = {"$group": {"_id":"$name",
                            "avgPriceUSD": {"$avg": "$quote.USD.price"},
                            "avgVolume24h": {"$avg": "$quote.USD.volume_24h"},
                            "avgMarketCap(USD)": {"$avg": "$quote.USD.market_cap"}}}

        project= {"$project": {"_id":0,
                                "name":"$_id",
                                "avgPriceUSD":1,
                                "avgVolume24h":1,
                                "avgMarketCap(USD)":1}}
        
        sort={"$sort": {"avgPriceUSD": -1,
                        "avgVolume24h": -1}}
        
        pipeline=[group, project, sort]
        results = self.collection.aggregate(pipeline)
        df=pd.DataFrame()
        for item in results:
            df=pd.concat([df, pd.json_normalize(item)], ignore_index=True)
        df=df.reindex(columns=['name', 'avgPriceUSD', 'avgVolume24h', 'avgMarketCap(USD)'])
        self.write_to_output(df, title)

    def price_change(self, crypto_name:str, title:str):
        """
        This function calculates the percentage change in price of a cryptocurrency,
        and writes the dataframe into a text file as output.

        Args:
            crypto_name (str): Name of the cryuptocurrency to perform aggregations on.
            title (str): Title of the Dataframe to be included in the output.
        """


        match = {"$match": {"name":f"{crypto_name}"}}

        last_fetched_price= {"$setWindowFields": {"partitionBy": "null",
                                "sortBy": {"datetime_fetched": 1},
                                "output": {"last_fetched_price": {"$shift": {"output": "$quote.USD.price",
                                                                "by": -1,
                                                                "default": 0}}}}}
        project= {"$project": {"_id":0,
                                "Price(USD)":"$quote.USD.price",
                                "datetime_fetched":1,
                                "last_fetched_price":1, 
                                "%_Change_Price":{"$multiply":[{"$cond":[{ "$eq":[ "$last_fetched_price", 0 ]}, 0, {"$divide":[{"$subtract": ["$quote.USD.price", "$last_fetched_price"]}, "$last_fetched_price"]}]},100] }}}
        sort={"$sort": {"last_fetched_price": -1}}
        pipeline=[match,last_fetched_price,project, sort]
        results = self.collection.aggregate(pipeline)
        df=pd.DataFrame()
        for item in results:
            df=pd.concat([df, pd.json_normalize(item)], ignore_index=True)
        df=df.reindex(columns=['datetime_fetched', 'Price(USD)', 'last_fetched_price', '%_Change_Price'])
        self.write_to_output(df=df, title=title)

    def price_volume_change(self, crypto_name:str, title:str):
        """
        This function calculates the percentage change in price and 24h volume change
        of a cryptocurrency, and writes the dataframe into a text file as output.

        Args:
            crypto_name (str): Name of the cryptocurrency.
            title (str): Title of the Dataframe to be included in the output.
        """
        match = {"$match": {"name":crypto_name}}

        lag_vol= {"$setWindowFields": {"partitionBy": "null",
                                "sortBy": {"datetime_fetched": 1},
                                "output": {"last_fetched_volume": {"$shift": {"output": "$quote.USD.volume_24h",
                                                                "by": -1,
                                                                "default": 0}}}}}
        lag_pri= {"$setWindowFields": {"partitionBy": "null",
                                "sortBy": {"datetime_fetched": 1},
                                "output": {"last_fetched_price": {"$shift": {"output": "$quote.USD.price",
                                                                "by": -1,
                                                                "default": 0}}}}}

        project= {"$project": {"_id":0,
                                "volume_24h":"$quote.USD.volume_24h",
                                "Price(USD)":"$quote.USD.price",
                                "datetime_fetched":1,
                                "last_fetched_volume":1, 
                                "last_fetched_price":1,
                                "%_Change_volume":{"$multiply":[{"$cond":[{ "$eq":[ "$last_fetched_volume", 0 ]}, 0, {"$divide":[{"$subtract": ["$quote.USD.volume_24h", "$last_fetched_volume"]}, "$last_fetched_volume"]}]},100]},
                                "%_Change_price":{"$multiply":[{"$cond":[{ "$eq":[ "$last_fetched_price", 0 ]}, 0, {"$divide":[{"$subtract": ["$quote.USD.price", "$last_fetched_price"]}, "$last_fetched_price"]}]},100]},
                                "change_price_vs_change_vol":{"$multiply":[{"$cond":[{"$eq":[ "$last_fetched_volume", 0 ]}, 0, 
                                                                                     {"$divide":[{"$divide":[{"$abs":{"$subtract": ["$quote.USD.price", "$last_fetched_price"]}}, "$last_fetched_price"]}, {"$divide":[{"$abs":{"$subtract": ["$quote.USD.volume_24h", "$last_fetched_volume"]}}, "$last_fetched_volume"]}]}]}, 
                                                                                     1]}
        }}
        pipeline=[match,lag_vol, lag_pri, project]

        results = self.collection.aggregate(pipeline)
        df=pd.DataFrame()    
        for item in results:
            df=pd.concat([df, pd.json_normalize(item)], ignore_index=True)
        df=df.reindex(columns=['datetime_fetched', 'Price(USD)', 'last_fetched_price', '%_Change_price', 'volume_24h', 'last_fetched_volume', '%_Change_volume', 'change_price_vs_change_vol'])
        self.write_to_output(df=df, title=title)

    def age_to_mc(self, title:str):
        """
        This function calculates the ratio of age to market cap dominance of cryptocurrencies,
        and writes the dataframe into a text file as output.

        Args:
            title (str): Title of the Dataframe to be included in the output.
        """

        group = {"$group": {"_id":"$name",
                            "DateAdded": {"$min": "$date_added"},
                            "datetime_fetched": {"$max": "$datetime_fetched"},
                            "avgMarketCapDominance": {"$avg": "$quote.USD.market_cap_dominance"}}}

        project= {"$project": {"_id":0,
                                "name":"$_id",
                                "DateAdded":1,
                                "datetime_fetched":1,
                                "Age(years)": {"$divide":[{"$subtract": [{"$toDate":"$datetime_fetched"}, {"$toDate":"$DateAdded"}]}, 31536000000]},
                                "Market_Cap_Dominance":"$avgMarketCapDominance",
                                "MCDominance_To_Age":{"$divide":["$avgMarketCapDominance", {"$divide":[{"$subtract": [{"$toDate":"$datetime_fetched"}, {"$toDate":"$DateAdded"}]}, 31536000000]}]}}}
        
        sort={"$sort": {"Age(years)": -1}}

        pipeline=[group, project, sort]
        results = collection.aggregate(pipeline)

        df=pd.DataFrame()    
        for item in results:
            df=pd.concat([df, pd.json_normalize(item)], ignore_index=True)
        df=df.reindex(columns=['name', 'DateAdded', 'datetime_fetched', 'Age(years)', 'Market_Cap_Dominance', 'MCDominance_To_Age'])
        self.write_to_output(df=df, title=title)

    def fdv_to_mc(self, title:str):
        """
        This function calculates the ratio of fully diluted market cap to the 
        current market cap of cryptocurrencies, and writes the dataframe into a text file as output.

        Args:
            title (str): Title of the Dataframe to be included in the output.
        """

        group = {"$group": {"_id":"$name",
                            "avgFDV(USD)": {"$avg": "$quote.USD.fully_diluted_market_cap"},
                            "avgMarketCap(USD)": {"$avg": "$quote.USD.market_cap"}}}

        project= {"$project": {"_id":0,
                            "name":"$_id",
                            "avgFDV(USD)":1,
                            "avgMarketCap(USD)":1,
                            "FDV_To_MarketCap":{"$divide":["$avgFDV(USD)", "$avgMarketCap(USD)"]}}}
        
        sort={"$sort": {"FDV_To_MarketCap": -1}}

        pipeline=[group, project, sort]
        results = self.collection.aggregate(pipeline)

        df=pd.DataFrame()    
        for item in results:
            df=pd.concat([df, pd.json_normalize(item)], ignore_index=True)
        df=df.reindex(columns=['name', 'avgFDV(USD)', 'avgMarketCap(USD)', 'FDV_To_MarketCap'])
        self.write_to_output(df=df, title=title)

    def delete_lastest_records(self):
        """
        This function deletes the latest entries of the day, for the last hour of fetching data.
        """

        results = self.collection.distinct("datetime_fetched")
        max_hour=""
        for item in results:
            hr = item[11:13]
            if hr>max_hour: max_hour=hr
        query={"$expr": {"$eq": [{"$substr": ["$datetime_fetched", 11, 2]}, max_hour]}}
        results=collection.delete_many(query)
        ti=datetime.now().strftime("%H:%M:%S")
        logging.info(f"Total number of deleted docs:{results.deleted_count}, at time {ti}")

collection=data_ingestion(interval_mins=5, num_runs=12, entries=20, DB_name="Crypto_db", COLL_name="Crypto_coll")

aggregations_obj=aggregations(collection=collection)
ti=datetime.now().strftime("%H:%M:%S")
logging.info(f"Created object of aggregations class at {ti}")

ti=datetime.now().strftime("%H:%M:%S")
logging.info(f"Calling aggregation functions, starting at {ti}")
aggregations_obj.agg_avg(title="Cryptocurrencies average data")
aggregations_obj.age_to_mc(title="Ratio of Market Cap Dominance of a crypto to its Age")
aggregations_obj.fdv_to_mc(title="Ratio of Fully Diluted Market Cap to current Market Cap")
for name in collection.distinct("name"):
    aggregations_obj.price_change(crypto_name=name,title=f"% Change in price of {name}")
    aggregations_obj.price_volume_change(crypto_name=name, title=f"% Change in price and volume of {name}")
ti=datetime.now().strftime("%H:%M:%S")
logging.info(f"Finished aggregations. Output file generated at {ti}")

aggregations_obj.delete_lasthour_records()

