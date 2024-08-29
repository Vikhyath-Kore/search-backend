"""
This file contains functions to process and then insert \
the received data into Elastic.
"""

from celery import Celery, current_task
from celery.utils.log import get_task_logger
from elasticsearch import Elasticsearch
from FlagEmbedding import BGEM3FlagModel
import torch
import validators

import json, copy

import send
from config import config

model = BGEM3FlagModel("BAAI/bge-m3", use_fp16=True)
torch.set_num_threads(1)

logger = get_task_logger(__name__)

celery_app = Celery(
    "tasks",
    broker=config["BROKER_URL"],
    backend=config["BACKEND_URL"],
)

es = Elasticsearch(config["ES_URL"])


def split_aggregated_fields(records: dict):
    """
    Helper function to split comma-separated aggregated fields \
    into a list of items.
    This list is hardcoded and is expected to be provided.
    """
    fields_to_split = ["actors", "director"]
    for field_to_split in fields_to_split:
        if "," in records[field_to_split]:
            records[field_to_split] = [
                item.strip() for item in records[field_to_split].split(",")
            ]
    return records


def embed_json(record: dict):
    """
    Helper function to select the fields to embed and embed the json.
    Don't consider a field in embedding if:
    1. It is an id field.
    2. It is a URL Field.
    3. ...
    """
    to_pop = []
    for key in record.keys():
        if key == "id":
            to_pop.append(key)
            continue
        result = validators.url(record[key])
        if not isinstance(result, validators.ValidationError):
            to_pop.append(key)

    logger.info("Skipping these fields during embedding:" + str(to_pop))
    for pop in to_pop:
        record.pop(pop)
    json_record = json.dumps(record)
    emb = model.encode(json_record, batch_size=12, max_length=512)["dense_vecs"]
    return emb


@celery_app.task()
def insert(json_data):
    """
    Celery Function to insert the json into ES.
    """
    try:
        req = current_task.request
    except Exception as _:
        send.send_msg(0, "Invalid Request")
        logger.info("Invalid Request")
        return "Invalid Request"

    send.send_msg(req.id, "Inprocess")  # type: ignore
    logger.info("Valid Request")
    # for i in range(10):
    # logger.info(i)
    # time.sleep(1)

    try:
        for index in json_data.keys():
            for record in json_data[index]:
                logger.info("Splitting Aggregated Fields")
                record = split_aggregated_fields(record)  # Split aggregated fields.
                logger.info("Done")
                logger.info("Inserting into ES.")
                es.index(
                    index=index, id=record["id"], document=record
                )  # Indexing into ES
        logger.info("Added json to ES!")
        send.send_msg(req.id, "Completed")  # type: ignore
    except Exception as _:
        logger.info("Insert into ES Failed.")
        send.send_msg(req.id, "Failed")  # type: ignore
        return "Insert into ES Failed."

    return "Insertion Complete."


@celery_app.task()
def insert_emb(json_data):
    """
    Celery function to insert json + embeddings of relevant fields \
    into ES.
    """
    try:
        req = current_task.request
    except Exception as _:
        logger.info("Invalid Request")
        send.send_msg(0, "Invalid Request")
        return "Invalid Request"

    send.send_msg(req.id, "Inprocess")  # type: ignore
    logger.info("Valid Request")

    try:
        for index in json_data.keys():
            for record in json_data[index]:
                logger.info(f"Currently on id: {record['id']}")
                logger.info("Embedding the data.")
                record_to_emb = copy.deepcopy(record)
                emb = embed_json(record=record_to_emb)  # Embedding Step
                logger.info("Embedding Successful.")
                logger.info("Now insert into ES.")
                record["embedding"] = emb  # Adding embedding to original record
                es.index(
                    index=index + "_emb", id=record["id"], document=record
                )  # Indexing into ES
        logger.info("Added all embeddings to ES!")
        send.send_msg(req.id, "Completed")  # type: ignore
    except Exception as _:
        logger.info("Insert into ES Failed.")
        send.send_msg(req.id, "Failed")  # type: ignore
        return "Insert into ES Failed."

    return "Insertion Complete(Embedding)."
