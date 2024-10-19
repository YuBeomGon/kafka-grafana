import psutil
# import Gputil
import time
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Kafka broker and topic configuration
KAFKA_BROKER = 'kafka:9092'  # Docker Compose에서 kafka 서비스로 정의된 호스트 네임
TOPIC_NAME = 'system_metrics'

# Initialize Kafka producer
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        break
    except NoBrokersAvailable:
        print("Kafka broker not available. Retrying in 5 seconds...")
        time.sleep(5)

def collect_system_metrics():
    """
    Collects CPU, memory, and disk usage using psutil.
    Returns a dictionary of system metrics.
    """
    metrics = {
        'cpu_percent': psutil.cpu_percent(interval=1),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_usage_percent': psutil.disk_usage('/').percent,
        'timestamp': time.time()  # Add timestamp to track data time
    }
    
    # # GPU 정보 수집 (GPUtil 사용)
    # try:
    #     # gpus = GPUtil.getGPUs()
    #     if gpus:
    #         gpu_info = []
    #         for gpu in gpus:
    #             gpu_info.append({
    #                 'gpu_id': gpu.id,
    #                 'gpu_name': gpu.name,
    #                 'gpu_load': gpu.load * 100,  # GPU 사용률 (0-1 사이의 값을 퍼센트로 변환)
    #                 'gpu_memory_total': gpu.memoryTotal,
    #                 'gpu_memory_used': gpu.memoryUsed,
    #                 'gpu_memory_free': gpu.memoryFree,
    #                 'gpu_memory_percent': (gpu.memoryUsed / gpu.memoryTotal) * 100,  # GPU 메모리 사용률
    #                 'gpu_temperature': gpu.temperature  # GPU 온도
    #             })
    #         metrics['gpu_info'] = gpu_info
    # except Exception as e:
    #     print(f"Error collecting GPU data: {e}")
    
    return metrics

def send_metrics_to_kafka():
    """
    Collects system metrics and sends them to the Kafka topic.
    """
    while True:
        metrics = collect_system_metrics()
        print(f"Sending metrics to Kafka: {metrics}")
        producer.send(TOPIC_NAME, metrics)
        producer.flush()
        time.sleep(5)  # Wait for 5 seconds before sending the next batch of metrics

if __name__ == '__main__':
    send_metrics_to_kafka()