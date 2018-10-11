# Hive <-> CARTO connector as a service

This demo shows how to transfer data from a Hive data warehouse to CARTO. It's built on a basic [Flask](http://flask.pocoo.org/) API that disptaches the transfer request to workers using [Celery](http://www.celeryproject.org/). Using the same pattern defined in [hive2carto](https://github.com/andy-esch/hive-carto-connector/blob/2c8897159e152cebe38fa0ce894009c323fd6b1f/celery-queue/tasks.py#L24-L55), we could add many more connectors.

## Docker/Flask/Celery/Redis

A basic [Docker Compose](https://docs.docker.com/compose/) template for orchestrating a Flask application & a Celery queue using Redis as the backend.

### Installation

```bash
git clone https://github.com/andy-esch/hive-carto-connector.git
```

### Build & Launch

```bash
docker-compose up -d --build
```

This will expose the Flask application's endpoints on port `5000` as well as a flower server for monitoring workers on port `5555`

To add more workers:
```bash
docker-compose up -d --scale worker=5 --no-recreate
```

### Usage

Once the services are running, access the `/hive` endpoint at:

```
http://localhost:5000/hive?user=username&key=api_key&database=hive_database&table=hive_tablename
```

This will transfer the table `hive_tablename` in the Hive database `hive_database` to the CARTO user account `username` which as the API key `api_key`. For example, with user `eschbacher` with API key `abcdefg` will import the Hive table `gps_points` from database `traces`:

```
http://localhost:5000/hive?user=eschbacher&key=abcdefg&database=traces&table=gps_points
```


### Stopping

To shut down:

```bash
docker-compose down
```

If you would like to change the endpoints, update the code in [api/app.py](api/app.py)

Task changes should happen in [queue/tasks.py](celery-queue/tasks.py) 

---

## Origins

Originally from [https://github.com/mattkohl/docker-flask-celery-redis](https://github.com/mattkohl/docker-flask-celery-redis), adapted from [https://github.com/itsrifat/flask-celery-docker-scale](https://github.com/itsrifat/flask-celery-docker-scale)
