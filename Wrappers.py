import mysql.connector
import logging
import boto3
from botocore.exceptions import ClientError

from enum import Enum
import uuid
import sys

class Order_Status(Enum):
    Undefined = 0
    Filled = 1
    Expired = 2
    Cancelled = 3
  



logger = logging.getLogger(__name__)
def Init_Logger(container_name):
    # Ensure the logger doesn't propagate messages to the root or parent loggers
    logger.propagate = False
    # Set the logging level to INFO (or any level you prefer)
    logger.setLevel(logging.INFO)
    # Create a StreamHandler for stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    # Create a formatter
    formatter = logging.Formatter(container_name + ': %(levelname)s - %(message)s')
    stdout_handler.setFormatter(formatter)
    # Add the handler to the logger
    logger.addHandler(stdout_handler)


def DB_Connect():
  try:
    mydb = mysql.connector.connect(
      host=db_host_name,
      user=db_user_name,
      password=db_pw,
      database = db_schema
    )
  except mysql.connector.Error as error:
    logger.exception("DB_Connect Failed: %s", error.msg)
    return None
    
  else:
     return mydb
  

def Send_Message(sqs_queue_name, message_body,message_group, unique_id, message_attributes=None):
  
  
  if not message_attributes:
        message_attributes = {}

  try:
        sqs = boto3.resource('sqs',region_name=sqs_region )
        queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
        response = queue.send_message(
            MessageBody=message_body,
            MessageAttributes=message_attributes,
            MessageGroupId=message_group,
            MessageDeduplicationId=unique_id
        )
  except ClientError as error:
        logger.exception("Send message failed: %s", message_body)
        return False
  else:
        return True
  


def receive_messages(sqs_queue_name,max_number, wait_time):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
   
  
    try:
        sqs = boto3.resource('sqs',region_name=sqs_region )
        queue = sqs.get_queue_by_name(QueueName=sqs_queue_name)
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            #MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
           # VisibilityTimeout=10
        )
        #for msg in messages:
        #    logger.info("Received message: %s: %s", msg.message_id, msg.body)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        return None
    else:
        return messages


def Send_Message_Price(symbol,price):
    body = symbol + ";" + str(price)
    unique_id = str(uuid.uuid1()).replace('-','')
    Send_Message(sqs_assetprices_name,body,"group1",unique_id)
  
  
def Send_Message_Order(queue,broker,order_id):
     message_body = str(broker) +";" + str(order_id)
     logger.info("Sending message, order id: %s, " + message_body,order_id)
     Send_Message(queue, message_body,"group1",str(order_id))          

def Parse_Order_Message(input ):
    try:
        result = str(input).split(';')
        if len(result)!= 2:
            logger.error("Parse_Order_message,invalid message received: %s", input)
            return None
        return (result[0], int(result[1]))  
    except Exception as general_error:  # This will catch any type of Exception
        logger.exception("Parse_Order_Message (General Error): %s", str(general_error))
        return None
