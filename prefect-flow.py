# prefect flow goes here
from prefect import flow, task, get_run_logger
import boto3, requests, time

UVA_ID = "cqb3tc"
EXPECTED_COUNT = 21

@task # populate the SQS queue by hitting the professor's API
def populate_queue(): 
    logger = get_run_logger()
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
    logger.info(f"hitting the prof's API: {UVA_ID}")
    response = requests.post(url)
    response.raise_for_status() # if anything is wrong
    payload = response.json()
    queue_url = payload["sqs_url"]
    logger.info(f"SQS populated: {queue_url}") 

    return queue_url

@task # receive messages incrementally from the SQS queue
def receive_messages_incrementally(queue_url, expected_count=EXPECTED_COUNT):
    logger = get_run_logger()
    sqs = boto3.client("sqs", region_name="us-east-1")
    collected = []

    logger.info("started receiving messages")
    while len(collected) < expected_count:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
            WaitTimeSeconds=20 
        )

        if "Messages" not in resp:
            logger.info("waiting")
            continue

        for msg in resp["Messages"]:
            attrs = msg.get("MessageAttributes", {})
            order_no = int(attrs["order_no"]["StringValue"])
            word = attrs["word"]["StringValue"]

            collected.append((order_no, word))

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"]
            )
            logger.info(f"stored and deleted message {order_no}: {word}")

        logger.info(f"{len(collected)}/{expected_count} messages collected so far.")

    logger.info("✅ all messages collected!")
    return collected

@task # reassemble the phrase and submit it
def reassemble_and_submit(messages):
    logger = get_run_logger()
    messages.sort(key=lambda x: x[0])
    phrase = " ".join(word for _, word in messages)
    logger.info(f"full phrase: {phrase}")

    sqs = boto3.client("sqs", region_name="us-east-1")
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    response = sqs.send_message(
        QueueUrl=submit_url,
        MessageBody="DP2 submission",
        MessageAttributes={
            "uvaid": {"DataType": "String", "StringValue": UVA_ID},
            "phrase": {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "prefect"}
        }
    )
    logger.info(f"submission response: {response}")
    return phrase

@flow # main flow function
def dp2_prefect_cqb3tc():
    queue_url = populate_queue()
    messages = receive_messages_incrementally(queue_url)
    phrase = reassemble_and_submit(messages)
    print(f"submitted phrase: {phrase}")

if __name__ == "__main__":
    dp2_prefect_cqb3tc()
