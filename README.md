
### USING
- spark-2.3.0-bin-hadoop2.7
- Python 2.7
- Java 8
- Flask
- CherryPy

### Details
- engine is a python for trainning data recommend by .csv.
- app.py is a Flask web application that defines a RESTful-like API around the engine.
- server.py initialises a CherryPy webserver after creating a Spark context and Flask web app using the previous.

### Setup server
- run download_dataset.sh for download data-recommend.
- run start_server.sh for submit pyspark.
- download spark 
  wget https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz && tar xf spark-2.3.0-bin-hadoop2.7.tgz
- Run background on vps: nohup python server.py
