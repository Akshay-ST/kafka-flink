from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaRecordDeserializationSchema
)
from pyflink.common.serialization import SimpleStringSchema
import json

def categorize_customer(data):
    """ Parse customer JSON and categorize into different topics. """
    customer = json.loads(data)
    if customer['age'] < 25:
        return "target-audience-topic", json.dumps(customer)
    else:
        return "old-folks-topic", json.dumps(customer)

# Initialize Flink environment
env = StreamExecutionEnvironment.get_execution_environment()

# Kafka Source (Read from customer-input)
source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("customer-input") \
    .set_group_id("flink-group") \
    .set_value_only_deserializer(KafkaRecordDeserializationSchema.value_only(SimpleStringSchema())) \
    .build()

# Kafka Sinks (Write to different topics)
target_audience_sink = KafkaSink.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_record_serializer(KafkaRecordSerializationSchema.value_only(SimpleStringSchema())) \
    .set_topic("target-audience-topic") \
    .build()

old_folks_sink = KafkaSink.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_record_serializer(KafkaRecordSerializationSchema.value_only(SimpleStringSchema())) \
    .set_topic("old-folks-topic") \
    .build()

# Read data from Kafka
ds = env.from_source(source, watermark_strategy=None, source_name="Kafka Source")

# Categorize Customers
categorized_ds = ds.map(categorize_customer)

# Split the stream into two different sinks
target_audience_stream = categorized_ds \
    .filter(lambda x: x[0] == "target-audience-topic") \
    .map(lambda x: x[1])

old_folks_stream = categorized_ds \
    .filter(lambda x: x[0] == "old-folks-topic") \
    .map(lambda x: x[1])

# Send to respective Kafka topics
target_audience_stream.sink_to(target_audience_sink)
old_folks_stream.sink_to(old_folks_sink)

# Execute Flink Job
env.execute("Customer Age Categorization - Multi Topic")
