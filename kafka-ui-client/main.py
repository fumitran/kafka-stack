"""
Ví dụ sử dụng Kafka UI Client
"""
import json
from kafka_ui_client import KafkaUIClient
from config import KafkaUIConfig


def print_section(title: str):
    """In tiêu đề section"""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def print_json(data, indent=2):
    """In JSON đẹp"""
    print(json.dumps(data, indent=indent, ensure_ascii=False))


def main():
    """Hàm main với các ví dụ sử dụng"""
    
    # Khởi tạo config
    # Cách 1: Dùng SESSION cookie (copy từ trình duyệt Kafka UI)
    # config = KafkaUIConfig(
    #     base_url="http://localhost:8080",
    #     session_token="your_session_value_here",
    # )

    # Cách 2: Dùng biến môi trường
    # export KAFKA_UI_SESSION=your_session_value_here
    # config = KafkaUIConfig(base_url="http://localhost:8080")

    # Cách 3: Dùng Basic Auth (nếu Kafka UI bật basic auth)
    # config = KafkaUIConfig(
    #     base_url="http://localhost:8080",
    #     username="your_username",
    #     password="your_password"
    # )

    # Mặc định: dùng giá trị trong config.py (username/password hoặc SESSION từ env)
    config = KafkaUIConfig(base_url="http://localhost:8080")
    client = KafkaUIClient(config)
    
    try:
        # 1. Lấy thông tin clusters
        print_section("1. DANH SÁCH CLUSTERS")
        clusters = client.get_clusters()
        print_json(clusters)
        
        # 2. Lấy thông tin cluster cụ thể
        print_section("2. THÔNG TIN CLUSTER")
        cluster_info = client.get_cluster_info()
        print_json(cluster_info)
        
        # 3. Lấy danh sách topics
        print_section("3. DANH SÁCH TOPICS")
        topics = client.get_topics()
        print(f"Tổng số topics: {len(topics)}")
        for topic in topics[:10]:  # Hiển thị 10 topics đầu tiên
            print(f"  - {topic.get('name')} ({topic.get('partitionsCount', 0)} partitions)")
        
        # 4. Lấy thông tin chi tiết về một topic
        if topics:
            topic_name = topics[0].get('name')
            print_section(f"4. THÔNG TIN CHI TIẾT TOPIC: {topic_name}")
            topic_details = client.get_topic_details(topic_name)
            print_json(topic_details)
            
            # 5. Lấy thống kê topic
            print_section(f"5. THỐNG KÊ TOPIC: {topic_name}")
            stats = client.get_topic_statistics(topic_name)
            print_json(stats)
            
            # 6. Lấy messages từ topic
            print_section(f"6. MESSAGES TỪ TOPIC: {topic_name} (10 messages đầu)")
            messages = client.get_topic_messages(topic_name, limit=10)
            print(f"Số lượng messages: {len(messages)}")
            for msg in messages[:5]:  # Hiển thị 5 messages đầu
                print(f"\n  Partition: {msg.get('partition')}, Offset: {msg.get('offset')}")
                print(f"  Key: {msg.get('key')}")
                print(f"  Value: {msg.get('value')[:100]}..." if len(str(msg.get('value', ''))) > 100 else f"  Value: {msg.get('value')}")
        
        # (Tạm thời bỏ qua brokers, consumer groups, metrics để tránh lỗi version API)

    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        print("\n💡 Gợi ý:")
        print("  1. Đảm bảo Kafka UI đang chạy trên http://localhost:8080")
        print("  2. Kiểm tra kết nối mạng")
        print("  3. Nếu có authentication, cung cấp username và password")


if __name__ == "__main__":
    main()

