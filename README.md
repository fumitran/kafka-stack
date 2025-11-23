# Kafka Stack - Complete Development Environment

Một stack Kafka hoàn chỉnh với Docker Compose, bao gồm Kafka cluster, monitoring tools, và Python demo applications.

## 📋 Tổng quan

Project này cung cấp một môi trường phát triển Kafka đầy đủ với:
- **Kafka Cluster** (2 brokers) với Zookeeper
- **Monitoring**: Prometheus + Grafana
- **Kafka Exporter**: Thu thập metrics từ Kafka
- **Kafka UI**: Giao diện web để quản lý và xem Kafka
- **Python Demo**: Producer và Consumer examples

## 🏗️ Kiến trúc

```
┌─────────────┐
│  Zookeeper  │
└──────┬──────┘
       │
   ┌───┴───┐
   │       │
┌──▼──┐ ┌──▼───┐
│Kafka│ │Kafka2│ (Cluster với 2 brokers)
└──┬──┘ └──┬───┘
   │       │
   └───┬───┘
       │
┌──────▼────────┐
│ Kafka Exporter│
└──────┬────────┘
       │
┌──────▼────────┐
│  Prometheus   │
└──────┬────────┘
       │
┌──────▼────────┐
│   Grafana     │
└───────────────┘
```

## 🚀 Các Services

| Service | Port | Mô tả |
|---------|------|-------|
| Zookeeper | 2181 | Quản lý metadata và coordination |
| Kafka Broker 1 | 9092 | Kafka broker đầu tiên |
| Kafka Broker 2 | 9093 | Kafka broker thứ hai |
| Kafka Exporter | 9308 | Expose Kafka metrics cho Prometheus |
| Prometheus | 9090 | Time-series database và monitoring |
| Grafana | 3000 | Dashboard visualization |
| Kafka UI | 8080 | Web UI để quản lý Kafka |

## 📦 Yêu cầu

- Docker và Docker Compose
- Python 3.8+ (cho Python demo)
- Git

## 🔧 Cài đặt và Chạy

### 1. Clone repository

```bash
git clone https://github.com/fumitran/kafka-stack.git
cd kafka-stack
```

### 2. Khởi động tất cả services

```bash
docker-compose up -d
```

Kiểm tra trạng thái các containers:

```bash
docker-compose ps
```

### 3. Kiểm tra logs

```bash
# Xem logs của tất cả services
docker-compose logs -f

# Xem logs của một service cụ thể
docker-compose logs -f kafka
```

### 4. Dừng services

```bash
docker-compose down
```

Để xóa cả volumes (dữ liệu):

```bash
docker-compose down -v
```

## 🐍 Python Producer/Consumer Demo

### Cài đặt dependencies

```bash
cd kafka-pytho-demo
python3 -m venv venv
source venv/bin/activate  # Trên Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Chạy Producer

Producer sẽ gửi messages mỗi 30 giây vào topic `fumitran`:

```bash
python producer.py
```

### Chạy Consumer

Consumer sẽ đọc messages từ topic `fumitran`:

```bash
python consumer.py
```

## 🌐 Truy cập các Services

- **Kafka UI**: http://localhost:8080
  - Xem topics, messages, consumer groups
  - Quản lý cluster
  
- **Grafana**: http://localhost:3000
  - Username: `admin`
  - Password: `admin` (đổi ngay lần đầu đăng nhập)
  - Thêm Prometheus data source: `http://prometheus:9090`

- **Prometheus**: http://localhost:9090
  - Query metrics, xem targets

## 📁 Cấu trúc Project

```
kafka-stack/
├── docker-compose.yml          # Cấu hình tất cả services
├── prometheus.yml              # Cấu hình Prometheus
├── .gitignore                  # Git ignore file
├── README.md                   # File này
└── kafka-pytho-demo/
    ├── producer.py             # Python producer example
    ├── consumer.py             # Python consumer example
    ├── requirements.txt        # Python dependencies
    └── venv/                   # Virtual environment (gitignored)
```

## 🔍 Sử dụng Kafka UI

1. Mở http://localhost:8080
2. Cluster `local` đã được cấu hình sẵn
3. Bạn có thể:
   - Xem danh sách topics
   - Xem messages trong topics
   - Tạo topics mới
   - Xem consumer groups
   - Monitor cluster health

## 📊 Monitoring với Grafana

1. Đăng nhập vào Grafana (http://localhost:3000)
2. Thêm Prometheus data source:
   - URL: `http://prometheus:9090`
   - Access: Server (default)
3. Import dashboard hoặc tạo dashboard mới
4. Query metrics từ Kafka Exporter:
   - `kafka_broker_info`
   - `kafka_topic_partitions`
   - `kafka_consumer_lag_sum`
   - Và nhiều metrics khác...

## 🛠️ Các lệnh hữu ích

### Tạo topic mới

```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 2
```

### Liệt kê topics

```bash
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

### Xem messages trong topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fumitran \
  --from-beginning
```

### Gửi message test

```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic fumitran
```

## ⚙️ Cấu hình

### Thay đổi số partitions mặc định

Sửa trong `docker-compose.yml`:
```yaml
KAFKA_NUM_PARTITIONS: 3  # Thay đổi từ 1
```

### Thay đổi replication factor

Sửa trong `docker-compose.yml`:
```yaml
KAFKA_DEFAULT_REPLICATION_FACTOR: 2  # Thay đổi từ 1
```

## 🐛 Troubleshooting

### Kafka không khởi động được

- Kiểm tra Zookeeper đã chạy: `docker-compose logs zookeeper`
- Kiểm tra ports có bị conflict không: `lsof -i :9092`

### Consumer không nhận được messages

- Kiểm tra topic đã tồn tại chưa
- Kiểm tra consumer group ID
- Xem logs: `docker-compose logs kafka`

### Prometheus không scrape được metrics

- Kiểm tra Kafka Exporter: http://localhost:9308/metrics
- Kiểm tra cấu hình trong `prometheus.yml`

## 📝 Notes

- Topic `fumitran` được sử dụng trong Python demo
- Consumer group: `my-analytics-group`
- Messages được format dưới dạng JSON
- Producer gửi message mỗi 30 giây

## 📄 License

MIT

## 👤 Author

fumitran

---

**Happy Kafka-ing! 🚀**

