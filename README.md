## Loading CoinMarketCap API data into MongoDB and performing aggregations using PyMongo
--------------------------------------
This repository contains the Python file, `main.py`, with the script for the entire program.<br />`logger.py` is the custom logging module used for logging and generating logs for every run in a separate folder.<br />`output_agg.txt` is the output text folder that stores the result for each run.

> The website [`CoinMarketCap`](https://coinmarketcap.com/) contains Cryptocurrency Prices data.<br />
The data can be downloaded using an API key from the website and the <mark style="background-color: blue">get</mark> function of the <mark style="background-color: blue">Sessions</mark> package in Python, by specifying the number of entries and the currency for conversion (here USD) as arguments.<br />
A free Cluster created in `MongoDB Atlas` is accessed using the <mark style="background-color: blue">MongoClient</mark> class of the <mark style="background-color: yellow">PyMongo</mark> module, where a Collection is created in a Database.<br />
A <mark style="background-color: blue">collection</mark> object is created using the <mark style="background-color: blue">connect_to_MongoDB</mark> function.<br />The code fetches a data array from the Web API every five minutes (for the number of times specified by the user; default runtime is for an hour) and uploads it to the MongoDB Cluster, where each element of the array is a separate Document of the Collection (the Documents are appended to the Collection each time).

Next, an output file is created as part of the <mark style="background-color: blue">aggregations</mark> class. There are five types of aggregations performed on the data:
* Average price of each Cryptocurrency, computed over the entire Collection
* Price Change for a particular Cryptocurrency over five minutes, as specified by the user
* Ratio of Price Change to Volume Change (over the last 24 hours) for a particular Cryptocurrency over five minutes, as specified by the user
* Ratio of Age of a Cryptocurrency to its Market Cap
* Ratio of Fully Diluted Market Cap of a Cryptocurrency to its Market Cap

> Each aggregation is performed using a <mark style="background-color: blue">pipeline</mark> as argument of the <mark style="background-color: blue">aggregate</mark> function of the <mark style="background-color: blue">collection</mark> object.<br />
The function returns an array of IDs for each returned JSON document, which is then converted into a <mark style="background-color: blue">Pandas</mark> Dataframe using the <mark style="background-color: blue">pd.json_normalize</mark> function along with <mark style="background-color: blue">pd.concat</mark>.<br />
The Dataframe is then converted to String and then written to the output text file using <mark style="background-color: blue">to_string</mark> function, with proper headings, for each aggregated result.<br />

Lastly, the Documents belonging to the latest run (last hour) can be deleted using a <mark style="background-color: blue">query</mark> as argument to the <mark style="background-color: blue">delete_many</mark> function of the <mark style="background-color: blue">collection</mark> object.


