import base64
import functions_framework
import io
import time
import json
import uuid
import random
import datetime
import string
import avro.schema
from avro.io import DatumReader, BinaryDecoder
from google.pubsub_v1.types.schema import Schema, Encoding, GetSchemaRequest, SchemaView
from google.pubsub_v1.services.publisher import PublisherClient
from google.pubsub_v1.services.schema_service import SchemaServiceClient
from google.pubsub_v1.types.pubsub import PubsubMessage, PublishRequest


PROJECT_ID  = "real-time-data-pipeline-457520"
TOPIC_ID = "orders-topic"

publisher    = PublisherClient()
topic_path   = publisher.topic_path(PROJECT_ID, TOPIC_ID)
topic        = publisher.get_topic(request={"topic": topic_path})
schema_name  = topic.schema_settings.schema
encoding     = topic.schema_settings.encoding
schema_client = SchemaServiceClient()
req = GetSchemaRequest(
    name=schema_name,
    view=SchemaView.FULL
)
schema_obj   = schema_client.get_schema(request=req)
avro_schema  = avro.schema.parse(schema_obj.definition)

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    data = base64.b64decode(cloud_event.data["message"]["data"])
    print(data)
    bout = io.BytesIO(data)
    decoder = BinaryDecoder(bout)
    reader = DatumReader(avro_schema)
    decoded_data = reader.read(decoder)

    # Perform some transformation on the data
    decoded_data['final_price'] = decoded_data['price'] * (1-decoded_data['discount'])
    # Simulate sending data to GCS
    print('Sending data to GCS')
    return "OK", 200