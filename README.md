


### USING

- spark-2.3.0-bin-hadoop2.7
wget https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz && tar xf spark-2.3.0-bin-hadoop2.7.tgz
- Python 2.7
- Java 8
- Flask
- CherryPy
### Details
- app.py is a Flask web application that defines a RESTful-like API around the engine.
- server.py initialises a CherryPy webserver after creating a Spark context and Flask web app using the previous.
