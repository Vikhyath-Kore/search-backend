"""
This file contains the Pika sender, which helps in providing \
status updates on the task.
"""

import pika
from pika import spec
from config import config


def send_msg(task_id, task_status):
    """
    Takes in the task_id and task_status and sends a message \
    with both parameters.
    To be ingested by the receiver.
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config["PIKA_HOST"])
    )
    channel = connection.channel()

    channel.queue_declare(queue="task-queue")

    channel.basic_publish(
        exchange="",
        routing_key="task-queue",
        body=f"Task ID:{task_id} \t Task Status:{task_status}",
        properties=pika.BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE),
    )

    connection.close()
