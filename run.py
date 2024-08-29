"""
This is the primary file which contains the API endpoints to be hit.
"""

from elasticsearch import Elasticsearch
from flask import Flask, Response, request
from celery import Celery
from FlagEmbedding import BGEM3FlagModel
from openai import OpenAI
import torch

import json, os
from dotenv import load_dotenv

from config import config

# Loading environment variables.
load_dotenv()

# Loading the embedding model.
model = BGEM3FlagModel("BAAI/bge-m3", use_fp16=True)
torch.set_num_threads(1)

app = Flask(__name__)

movie_app = Celery(
    "movie_worker",
    broker=config["BROKER_URL"],
    backend=config["BACKEND_URL"],
)

es = Elasticsearch(config["ES_URL"])

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

movie_app.set_default()


def NotFound404(msg):
    """
    Helper function for 404 messages.
    """
    return Response(msg, status=404, mimetype="application/json")


@app.route("/insert_emb", methods=["POST"])
def insert_emb():
    """
    Insert embeddings of the file into ES.
    """
    try:
        with open("Movies_DB.json") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        return NotFound404("Missing File.")

    if json_data:
        app.logger.info("Received File. Trying insert into ES.")
        try:
            resp = movie_app.send_task(
                "tasks.insert_emb", kwargs=({"json_data": json_data})
            )
            app.logger.info("Inserted!")
            app.logger.info(resp.id)
            return Response(
                f"Inserted into ES. task_id: {resp.id}",
                status=202,
                mimetype="application/json",
            )
        except:
            return Response(
                "Internal Server Error", status=501, mimetype="application/json"
            )
    else:
        return NotFound404("Missing File.")


@app.route("/insert", methods=["POST"])
def insert():
    """
    Insert file into ES.
    """
    try:
        with open("Movies_DB.json") as f:
            json_data = json.load(f)
    except FileNotFoundError:
        return NotFound404("Missing File.")

    if json_data:
        app.logger.info("Received File. Trying insert into ES.")
        try:
            resp = movie_app.send_task(
                "tasks.insert", kwargs=({"json_data": json_data})
            )
            app.logger.info("Inserted!")
            app.logger.info(resp.id)
            return Response(
                f"Inserted into ES. task_id: {resp.id}",
                status=202,
                mimetype="application/json",
            )
        except:
            return Response(
                "Internal Server Error", status=501, mimetype="application/json"
            )
    else:
        return NotFound404("Missing File.")


@app.route("/get_status/<task_id>", methods=["GET"])
def get_status(task_id):
    """
    Retrieve the status of a task using its task id.
    """
    app.logger.info(f"Fetching Status of task: {task_id}")
    status = movie_app.AsyncResult(task_id, app=movie_app).state
    app.logger.info(f"status: {status}")
    if status == "SUCCESS":
        return Response("Completed", status=200, mimetype="application/json")
    else:
        return Response("Processing", status=200, mimetype="application/json")


@app.route("/delete", methods=["DELETE"])
def delete():
    """
    Create an API to delete a record from the ElasticSearch index based on any field.
    """
    index = request.args.get("index")
    if index is None or index == "":
        return NotFound404("Index Not Found.")
    field = request.args.get("field")
    if field is None or field == "":
        return NotFound404("Field Not Found.")
    value = request.args.get("value")
    if value is None or value == "":
        return NotFound404("Value Not Found.")

    if es.indices.exists(index=index):
        req_body = {"constant_score": {"filter": {"term": {field + ".keyword": value}}}}
        result = dict(es.search(index=index, query=req_body).body)["hits"]["hits"]
        if result == []:
            return Response("Record not found", status=404, mimetype="application/json")
        for res in result:
            id_to_be_deleted = res["_id"]
            es.delete(index=index, id=id_to_be_deleted)
            app.logger.info(f"id:{id_to_be_deleted} has been deleted")
        return Response("Records Deleted.", status=200, mimetype="application/json")
    else:
        return NotFound404("Index Not Found.")


def get_unique_counts(index, field):
    req_body = {"type_count": {"cardinality": {"field": field + ".keyword"}}}
    return dict(es.search(index=index, size=0, aggs=req_body).body)["aggregations"][
        "type_count"
    ]["value"]


@app.route("/stats", methods=["GET"])
def get_stats():
    """
    Create an API to give a brief stats like how many unique genres, actors and directors are there in the entire dataset.
    """
    index = request.args.get("index")
    if index is None or index == "":
        return Response("Index not found", status=404, mimetype="application/json")
    if es.indices.exists(index=index):
        unq_genre_count = get_unique_counts(index=index, field="genres")
        unq_director_count = get_unique_counts(index=index, field="director")
        unq_actor_count = get_unique_counts(index=index, field="actors")

        response = {
            "Unique Genre Count": unq_genre_count,
            "Unique Director Count": unq_director_count,
            "Unique Actor Count": unq_actor_count,
        }
        app.logger.info(response)
        return Response(
            response=json.dumps(response), status=201, mimetype="application/json"
        )
    else:
        return NotFound404("Index Not Found.")


@app.route("/exact_search", methods=["GET"])
def exact_search():
    """
    Create an API to get specific movie related details based on the movie name.
    This API can be used for any field.
    Create an API to match and return all the documents acted by “Leonardo DiCaprio”.
    """
    # TODO: .keyword should only be used for keys with string values. See what to do for numeric values in the general case
    # TODO: Use Mongo to store status.
    index = request.args.get("index")
    if index is None or index == "":
        return NotFound404("Index Not Found.")
    field = request.args.get("field")
    if field is None or field == "":
        return NotFound404("Field Not Found.")
    value = request.args.get("value")
    if value is None or value == "":
        return NotFound404("Value Not Found.")

    if es.indices.exists(index=index):
        req_body = {"constant_score": {"filter": {"term": {field + ".keyword": value}}}}
        result = dict(es.search(index=index, query=req_body).body)["hits"]["hits"]
        return Response(
            response=json.dumps(result), status=201, mimetype="application/json"
        )
    else:
        return NotFound404("Index Not Found.")


@app.route("/vector_search", methods=["GET"])
def emb_search():
    """
    Create an answer API to answer from the data ingested (Use vector search by default)
    """
    index = request.args.get("index")
    if index is None or index == "":
        return NotFound404("Index Not Found.")
    query = request.args.get("query")
    if query is None or query == "":
        return NotFound404("Query Not Found.")
    size = request.args.get("size")
    if size is None or not size.isnumeric():
        return NotFound404("Size not Found.")
    size = int(size)

    # Retrieving Answers.
    if es.indices.exists(index=index):
        query_vector = model.encode(query, batch_size=12, max_length=512)["dense_vecs"]
        script_query = {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": query_vector},
                },
            }
        }
        results = dict(es.search(index=index, query=script_query, size=size).body)[
            "hits"
        ]["hits"]
        for result in results:
            result["_source"].pop("embedding", None)
    else:
        return NotFound404("Index Not Found.")
    
    # Generating Answers.
    app.logger.info("Connecting to OpenAI...")
    assistant_prompt = f"""
    You are provided with {size} json documents.
    {results}

    Based on these documents, answer the following. Do not use any additional information other than what is in the documents.
    """

    user_prompt = f"""
    {query}
    """
    try:
        response = client.chat.completions.create(
            model=config["OPENAI_GENERATION_MODEL"],
            messages=[
                {"role": "assistant", "content": assistant_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=512,
        )
        response_text = response.choices[0].message.content
        return Response(
            response=json.dumps(response_text) + "\n" + json.dumps(results),
            status=201,
            mimetype="application/json",
        )
    except Exception as e:
        app.logger.info(str(e))
        return Response(
            response="Unable to connect to OpenAI. Check the API Key.",
            status=500,
            mimetype="application/json",
        )


if __name__ == "__main__":
    app.run(debug=True)
