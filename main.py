import asyncio
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from hist_market_data.hist_api import HistApi
from hist_market_data.ws.ws_api import WsApi
from hist_market_data.hist_bar_const import (
    OHLCV_TIMESTAMP,
    OHLCV_OPEN,
    OHLCV_HIGH,
    OHLCV_LOW,
    OHLCV_CLOSE,
    OHLCV_VOLUME,
)

class DataService:
    def __init__(self, exchange, api_key, api_secret, kafka_config=None):
        self.exchange = exchange
        self.api_key = api_key
        self.api_secret = api_secret
        self.hist_api = HistApi(exchange)
        self.ws_api_manager = WsApi()

        # Initialize Kafka Producer
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'SSL',
            'ssl.ca.location': '/Users/jinshidiannao/Documents/asset_management/kafka_utils/certs/ca-cert.pem',  # Path to CA certificate
            'ssl.certificate.location': '/Users/jinshidiannao/Documents/asset_management/kafka_utils/certs/client-cert.pem', # Path to client certificate
            'ssl.key.location': '/Users/jinshidiannao/Documents/asset_management/kafka_utils/certs/client-key.pem',   # Path to client key
            ** (kafka_config if kafka_config else {})
        }
        self.producer = Producer(self.kafka_config)
        self.admin_client = AdminClient(self.kafka_config)

    def _create_topic_if_not_exists(self, topic_name, num_partitions=1, replication_factor=1):
        """ Creates a Kafka topic if it does not already exist. """
        topic_metadata = self.admin_client.list_topics(timeout=5).topics
        if topic_name not in topic_metadata:
            print(f"Topic '{topic_name}' does not exist. Creating it...")
            new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            futures = self.admin_client.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    print(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    print(f"Failed to create topic '{topic}': {e}")
        else:
            print(f"Topic '{topic_name}' already exists.")

    def _delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result. """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def _produce_to_kafka(self, topic, data):
        try:
            self.producer.produce(topic, key=str(data.get(OHLCV_TIMESTAMP)).encode('utf-8'), value=json.dumps(data).encode('utf-8'), callback=self._delivery_report)
            self.producer.poll(0) # Non-blocking poll
        except Exception as e:
            print(f"Failed to produce message to Kafka: {e}")

    async def subscribe_ohlcv(self, symbol, interval, topic):
        print(f"Subscribing to OHLCV for {symbol} ({interval}) on {self.exchange}")

        # Ensure topic exists
        self._create_topic_if_not_exists(topic))

        # 1. Fetch historical data
        print(f"Fetching historical OHLCV data for {symbol} ({interval})...")
        hist_data = self.hist_api.get_hist_bars(symbol, interval)
        if hist_data.data:
            for i in range(len(hist_data.data[OHLCV_TIMESTAMP])):
                ohlcv_entry = {
                    OHLCV_TIMESTAMP: hist_data.data[OHLCV_TIMESTAMP][i],
                    OHLCV_OPEN: hist_data.data[OHLCV_OPEN][i],
                    OHLCV_HIGH: hist_data.data[OHLCV_HIGH][i],
                    OHLCV_LOW: hist_data.data[OHLCV_LOW][i],
                    OHLCV_CLOSE: hist_data.data[OHLCV_CLOSE][i],
                    OHLCV_VOLUME: hist_data.data[OHLCV_VOLUME][i],
                }
                self._produce_to_kafka(topic, ohlcv_entry)
            print(f"Produced {len(hist_data.data[OHLCV_TIMESTAMP])} historical OHLCV entries to Kafka topic {topic}")
        else:
            print(f"No historical data found for {symbol} ({interval})")

        # 2. Subscribe to real-time updates
        def ws_ohlcv_callback(data):
            print(f"Received real-time OHLCV data for {symbol}: {data}")
            self._produce_to_kafka(topic, data)

        self.ws_api_manager.subscribe_ohlcv(self.exchange, symbol, interval, ws_ohlcv_callback)
        print(f"Subscribed to real-time OHLCV for {symbol} ({interval})")

    # Add methods for other data types (trades, depth) as needed

    def close(self):
        print("Flushing remaining Kafka messages...")
        self.producer.flush(30) # Flush messages with a 30-second timeout
        print("Kafka producer closed.")

async def main():
    # Example Usage:
    # Replace with your actual exchange, API keys, and Kafka config
    exchange = 'tiger'
    api_key = 'YOUR_TIGER_API_KEY'
    api_secret = 'YOUR_TIGER_API_SECRET'
    kafka_config = {
        # 'bootstrap.servers': 'your_kafka_broker:9092',
        # 'security.protocol': 'SSL',
        # 'ssl.ca.location': '/path/to/your/ca-cert.pem',
        # 'ssl.certificate.location': '/path/to/your/client-cert.pem',
        # 'ssl.key.location': '/path/to/your/client-key.pem',
    }

    data_service = DataService(exchange, api_key, api_secret, kafka_config)

    ohlcv_topic = f'{exchange}.ohlcv.AAPL.1min'
    await data_service.subscribe_ohlcv(symbol='AAPL', interval='1min', topic=ohlcv_topic)

    # Keep the main loop running to receive WebSocket updates
    try:
        while True:
            await asyncio.sleep(1) # Keep the event loop alive
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        data_service.close()

if __name__ == "__main__":
    asyncio.run(main())
