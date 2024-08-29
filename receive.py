"""
This file contains the Pika receiver, which waits and \
publishes messages sent by the sender.
"""
import pika, sys, os
from config import config


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config["PIKA_HOST"])
    )
    channel = connection.channel()

    channel.queue_declare(queue="task-queue")

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")

    channel.basic_consume(
        queue="task-queue", on_message_callback=callback, auto_ack=True
    )

    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
