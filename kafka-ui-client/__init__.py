"""
Kafka UI Client - Python client để kéo thông tin từ Kafka UI API
"""
from .kafka_ui_client import KafkaUIClient
from .config import KafkaUIConfig

__version__ = "1.0.0"
__all__ = ['KafkaUIClient', 'KafkaUIConfig']

