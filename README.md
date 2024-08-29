# Search Backend Onboarding Assignment

This is an assignment completed as part of onboarding to the SearchAssist team.
Refer to [this](https://docs.google.com/document/d/1pzKF6hu28xtdOkR7CBLxWCEts9fy_jpKliOmEth_d00/edit) for full details of the requirements.

### Versions

- Python: 3.9.7
- Flask: 3.0.3
- Celery: v5.4.0 (opalescent)
- ElasticSearch: 8.15.0
- RabbitMQ: 3.8.2
- MongoDB: 4.4.29 (shell)

### Endpoints

#### Regular
1. Inserting Json into ES: http://localhost:5000/insert
2. Retrieve the status of a task: http://localhost:5000/get_status/<task_id>
3. Delete a record: http://localhost:5000/delete?index=<index>&field=<field>&value=<value>
4. Return brief stats like how many unique genres, actors and directors are there in the entire dataset: http://localhost:5000/stats?index=movies
5. Conduct an exact match search on any field: http://localhost:5000/exact_search?index=<index>&field=<field>&value=<value>

#### Embedding-based
1. Insert Json + Embeddings into ES: http://localhost:5000/insert_emb
2. Answer any question based on the ingested data: http://localhost:5000/vector_search?index=<index>&size=<size>&query=<query>

Embedding model uses [bge-m3](https://huggingface.co/BAAI/bge-m3/tree/main) to embed and gpt-4o-mini-2024-07-18 for generation.

### Startup
Install RabbitMQ, ElasticSearch, MongoDB.

#### Python requirements
Create a virtual environment and install from the [requirements.txt](./requirements.txt) file.
```bash
pip install -r requirements.txt
```

##### RabbitMQ

```bash
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server
```

##### ElasticSearch

```bash
./bin/elasticsearch
```

##### Celery Tasks

```bash
celery -A tasks worker --loglevel=info --concurrency=2
```

##### RabbitMQ Queue Receiver

```bash
python receive.py
```

##### Main File

```bash
python run.py
```