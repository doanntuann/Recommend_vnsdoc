
# Recommendtion for website vnsdoc.com

### Requiment (recommend use)
- spark-2.3.0-bin-hadoop2.7
- Python 2.7
- Java 8
- Flask
- CherryPy

### Python file
- `engine.py` is a python for trainning data recommend by exten .csv.
- `app.py is` a Flask web application that defines a RESTful-like API around the engine.
- `server.py` initialises a CherryPy webserver after creating a Spark context and Flask web app using the previous.

### Setup server
- Run `download_dataset.sh` for download data-recommend. (after need change `dataset_path` in `server.py`)
- Run `start_server.sh` for submit pyspark.
- Download spark on host/vps/etc - unzip to root path.
- wget https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz && tar xf spark-2.3.0-bin-hadoop2.7.tgz
- `import package` requiment
- Run `python server.py`
- Run scrip server on background at VPS: `nohup python server.py` (if need)

### USING API
- domain/user_id/ratings/post_id: GET ratings with user_id and post_id
- domain/user_id/ratings/count: GET top recommendtion by user_id
- domain/user_id/ratings/ : POST ratings - post binary file (example binary file: `user_ratings.file`)

### TEST 
http://178.128.123.19

### Read more in here 
- https://github.com/jadianes/spark-movie-lens
