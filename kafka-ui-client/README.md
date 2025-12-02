# Kafka UI Client

Project Python để kéo thông tin từ Kafka UI API chạy trên `http://localhost:8080`.

## Cài đặt

1. **Cài đặt dependencies:**
```bash
pip install -r requirements.txt
```

2. **Cấu hình authentication (nếu có):**

   Có 2 cách để cấu hình username và password:

   **Cách 1: Sử dụng biến môi trường**
   ```bash
   export KAFKA_UI_USERNAME=your_username
   export KAFKA_UI_PASSWORD=your_password
   ```

   **Cách 2: Truyền trực tiếp trong code**
   ```python
   from config import KafkaUIConfig
   
   config = KafkaUIConfig(
       base_url="http://localhost:8080",
       username="your_username",
       password="your_password"
   )
   ```

## Sử dụng

### Ví dụ cơ bản

```python
from kafka_ui_client import KafkaUIClient
from config import KafkaUIConfig

# Khởi tạo config
config = KafkaUIConfig(base_url="http://localhost:8080")
client = KafkaUIClient(config)

# Lấy danh sách topics
topics = client.get_topics()
print(f"Tổng số topics: {len(topics)}")

# Lấy thông tin chi tiết về một topic
topic_details = client.get_topic_details("fumitran")
print(topic_details)

# Lấy messages từ topic
messages = client.get_topic_messages("fumitran", limit=10)
print(messages)
```

### Chạy ví dụ

```bash
python main.py
```

## Các tính năng

### 1. Cluster Information
- `get_clusters()` - Lấy danh sách tất cả clusters
- `get_cluster_info()` - Lấy thông tin chi tiết về cluster

### 2. Topics
- `get_topics()` - Lấy danh sách tất cả topics
- `get_topic_details(topic_name)` - Lấy thông tin chi tiết về topic
- `get_topic_messages(topic_name, limit, partition, ...)` - Lấy messages từ topic
- `get_topic_config(topic_name)` - Lấy cấu hình của topic
- `search_topics(search_term)` - Tìm kiếm topics
- `get_topic_statistics(topic_name)` - Lấy thống kê tổng hợp về topic

### 3. Brokers
- `get_brokers()` - Lấy danh sách brokers
- `get_broker_details(broker_id)` - Lấy thông tin chi tiết về broker

### 4. Consumer Groups
- `get_consumer_groups()` - Lấy danh sách consumer groups
- `get_consumer_group_details(group_id)` - Lấy thông tin chi tiết về consumer group
- `get_consumer_group_offsets(group_id)` - Lấy thông tin offsets của consumer group

### 5. Schemas
- `get_schemas()` - Lấy danh sách schemas (nếu có Schema Registry)

### 6. Metrics
- `get_metrics()` - Lấy metrics của cluster

## Cấu trúc Project

```
kafka-ui-client/
├── config.py              # Quản lý cấu hình
├── kafka_ui_client.py     # Client chính để tương tác với Kafka UI API
├── main.py                # Ví dụ sử dụng
├── requirements.txt       # Dependencies
└── README.md              # Tài liệu này
```

## Yêu cầu

- Python 3.7+
- Kafka UI đang chạy trên `http://localhost:8080`
- Quyền đọc (read) trên hệ thống Kafka

## Lưu ý

- Đảm bảo Kafka UI đã được khởi động và có thể truy cập được
- Nếu Kafka UI có authentication, bạn cần cung cấp username và password
- Một số API endpoints có thể không khả dụng tùy thuộc vào phiên bản Kafka UI

## Troubleshooting

### Lỗi kết nối
- Kiểm tra Kafka UI có đang chạy không: `curl http://localhost:8080/actuator/health`
- Kiểm tra firewall và network settings

### Lỗi authentication
- Đảm bảo username và password đúng
- Kiểm tra quyền truy cập của user

### Lỗi không tìm thấy cluster
- Kiểm tra tên cluster trong config có đúng không
- Sử dụng `get_clusters()` để xem danh sách clusters có sẵn

