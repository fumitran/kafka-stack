"""
Liệt kê toàn bộ thông tin chi tiết của consumer trong các topic có trong cluster chỉ định
(lấy ra thông tin consumer đang kết nối vào nó) và xuất kết quả vào file CSV.
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from kafka_ui_client import KafkaUIClient
from config import KafkaUIConfig

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_export.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def ensure_export_dir() -> str:
    """Đảm bảo tồn tại thư mục export, trả về đường dẫn tuyệt đối."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    export_dir = os.path.join(base_dir, "export")
    os.makedirs(export_dir, exist_ok=True)
    return export_dir


def sanitize_name_for_filename(name: str) -> str:
    """Chuẩn hóa tên để dùng trong tên file: chỉ giữ [A-Za-z0-9_.-], còn lại thay bằng '_'."""
    if not name:
        return "default"
    # Thay mọi chuỗi ký tự không hợp lệ bằng dấu gạch dưới
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip())
    return safe or "default"


def normalize_row(data: Dict[str, Any], fieldnames: List[str]) -> Dict[str, str]:
    """
    Chuyển dict sang dict string cho CSV.
    - Giá trị None -> "".
    - List / Dict -> json.dumps.
    """
    row: Dict[str, str] = {}
    for key in fieldnames:
        value = data.get(key)
        if value is None:
            row[key] = ""
        elif isinstance(value, (dict, list)):
            row[key] = json.dumps(value, ensure_ascii=False)
        else:
            row[key] = str(value)
    return row


def get_consumers_for_topic(
    client: KafkaUIClient,
    topic_name: str,
    cluster_name: str
) -> List[Dict[str, Any]]:
    """
    Lấy thông tin consumers đang consume một topic cụ thể.
    
    Args:
        client: KafkaUIClient instance
        topic_name: Tên topic
        cluster_name: Tên cluster
        
    Returns:
        List các consumer records cho topic này
    """
    consumers = []
    
    try:
        # Lấy danh sách consumer groups đang consume topic này
        consumer_groups = client.get_topic_consumer_groups(topic_name, cluster_name)
        
        if not consumer_groups:
            return consumers
        
        # Duyệt qua từng consumer group để lấy thông tin chi tiết
        for consumer_group in consumer_groups:
            consumer_group_id = consumer_group.get('groupId') or consumer_group.get('id')
            if not consumer_group_id:
                continue
            
            try:
                # Lấy thông tin chi tiết của consumer group
                try:
                    group_details = client.get_consumer_group_details(consumer_group_id, cluster_name)
                except Exception as e:
                    error_msg = str(e)
                    if '404' in error_msg or 'Not Found' in error_msg:
                        logger.debug(f"    Consumer group '{consumer_group_id}' không tồn tại hoặc không thể truy cập.")
                        continue
                    else:
                        logger.warning(f"    Không thể lấy chi tiết cho consumer group '{consumer_group_id}': {e}")
                        group_details = consumer_group
                
                # Lấy offsets để biết chi tiết partitions
                offsets_data = None
                try:
                    offsets_data = client.get_consumer_group_offsets(consumer_group_id, cluster_name)
                except Exception as e:
                    error_msg = str(e)
                    if '404' in error_msg or 'Not Found' in error_msg:
                        logger.debug(f"    Consumer group '{consumer_group_id}' không có offsets endpoint.")
                    else:
                        logger.debug(f"    Không thể lấy offsets cho consumer group '{consumer_group_id}': {e}")
                    
                    # Thử lấy từ group_details
                    if isinstance(group_details, dict) and 'offsets' in group_details:
                        offsets_data = group_details.get('offsets')
                
                # Parse offsets để lấy thông tin partitions
                offsets_list = []
                if offsets_data:
                    if isinstance(offsets_data, list):
                        offsets_list = offsets_data
                    elif isinstance(offsets_data, dict):
                        offsets_list = offsets_data.get('offsets', [])
                        if not offsets_list and 'topic' in offsets_data:
                            offsets_list = [offsets_data]
                
                # Lọc chỉ lấy offsets của topic này
                topic_offsets = []
                for offset_info in offsets_list:
                    if isinstance(offset_info, dict):
                        offset_topic = offset_info.get('topic')
                        if offset_topic == topic_name:
                            topic_offsets.append(offset_info)
                
                # Lấy thông tin members từ partitions trong group_details
                # Nếu không có offsets từ API, thử lấy từ partitions trong group_details
                if not topic_offsets and isinstance(group_details, dict) and 'partitions' in group_details:
                    partitions = group_details.get('partitions', [])
                    for partition_info in partitions:
                        if isinstance(partition_info, dict):
                            partition_topic = partition_info.get('topic')
                            if partition_topic == topic_name:
                                topic_offsets.append(partition_info)
                
                # Extract thông tin members từ partitions (chỉ lấy của topic này)
                # Tạo set để check trùng dựa trên (topic_name, consumer_group_id, consumer_id, host)
                seen_combinations = set()
                
                # Lấy thông tin từ partitions của topic này
                if isinstance(group_details, dict) and 'partitions' in group_details:
                    partitions = group_details.get('partitions', [])
                    for partition_info in partitions:
                        if isinstance(partition_info, dict):
                            partition_topic = partition_info.get('topic')
                            if partition_topic == topic_name:
                                consumer_id = partition_info.get('consumerId')
                                host = partition_info.get('host')
                                
                                # Tạo key để check trùng
                                if consumer_id:
                                    combination_key = (topic_name, consumer_group_id, consumer_id, host or '')
                                    
                                    # Chỉ thêm nếu chưa có
                                    if combination_key not in seen_combinations:
                                        seen_combinations.add(combination_key)
                                        
                                        # Tạo record với 4 trường cần thiết
                                        consumer_record = {
                                            'topicName': topic_name,
                                            'consumerGroupId': consumer_group_id,
                                            'consumerClientId': consumer_id or '',
                                            'consumerClientHost': host or '',
                                        }
                                        consumers.append(consumer_record)
                
                # Nếu không có partitions hoặc không tìm thấy consumer info trong partitions
                # Thử lấy từ topic_offsets
                if not consumers and topic_offsets:
                    for offset_info in topic_offsets:
                        if isinstance(offset_info, dict):
                            consumer_id = offset_info.get('consumerId')
                            host = offset_info.get('host')
                            
                            if consumer_id:
                                combination_key = (topic_name, consumer_group_id, consumer_id, host or '')
                                
                                if combination_key not in seen_combinations:
                                    seen_combinations.add(combination_key)
                                    
                                    consumer_record = {
                                        'topicName': topic_name,
                                        'consumerGroupId': consumer_group_id,
                                        'consumerClientId': consumer_id or '',
                                        'consumerClientHost': host or '',
                                    }
                                    consumers.append(consumer_record)
                
                # Nếu vẫn không có consumer info, tạo record với consumer info rỗng
                if not consumers:
                    combination_key = (topic_name, consumer_group_id, '', '')
                    if combination_key not in seen_combinations:
                        seen_combinations.add(combination_key)
                        consumer_record = {
                            'topicName': topic_name,
                            'consumerGroupId': consumer_group_id,
                            'consumerClientId': '',
                            'consumerClientHost': '',
                        }
                        consumers.append(consumer_record)
                
            except Exception as e:
                logger.warning(f"  Không thể xử lý consumer group '{consumer_group_id}' cho topic '{topic_name}': {e}")
                logger.debug(f"  Traceback: {traceback.format_exc()}")
                continue
                
    except Exception as e:
        error_msg = str(e)
        if '404' in error_msg or 'Not Found' in error_msg:
            # Topic không có consumer groups - đây là trường hợp bình thường
            logger.debug(f"  Topic '{topic_name}' không có consumer groups.")
        else:
            logger.warning(f"  Lỗi khi lấy consumer groups cho topic '{topic_name}': {e}")
    
    return consumers


def get_all_consumers_for_cluster(
    client: KafkaUIClient,
    topics: List[Dict[str, Any]],
    cluster_name: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Lấy thông tin tất cả consumers trong cluster, nhóm theo topic.
    
    Args:
        client: KafkaUIClient instance
        topics: Danh sách topics
        cluster_name: Tên cluster
        
    Returns:
        Dict với key là topic_name, value là list các consumer records
    """
    topic_consumers_map: Dict[str, List[Dict[str, Any]]] = {}
    
    logger.info(f"  Đang lấy consumer groups cho từng topic...")
    
    for idx, topic in enumerate(topics, 1):
        topic_name = topic.get('name')
        if not topic_name:
            continue
        
        try:
            logger.info(f"  [{idx}/{len(topics)}] Đang lấy consumer groups cho topic '{topic_name}'...")
            consumers = get_consumers_for_topic(client, topic_name, cluster_name)
            
            if consumers:
                topic_consumers_map[topic_name] = consumers
                logger.info(f"    → Tìm thấy {len(consumers)} consumer record(s)")
            else:
                logger.debug(f"    → Không có consumer groups")
                
        except Exception as e:
            logger.warning(f"  Lỗi khi lấy consumer groups cho topic '{topic_name}': {e}")
            continue
    
    return topic_consumers_map


def remove_duplicate_consumers(consumers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Loại bỏ các consumer records trùng lặp dựa trên 4 trường:
    topicName, consumerGroupId, consumerClientId, consumerClientHost
    
    Args:
        consumers: Danh sách consumer records
        
    Returns:
        Danh sách consumer records đã loại bỏ trùng lặp
    """
    seen = set()
    unique_consumers = []
    
    for consumer in consumers:
        topic_name = consumer.get('topicName', '')
        consumer_group_id = consumer.get('consumerGroupId', '')
        consumer_client_id = consumer.get('consumerClientId', '')
        consumer_client_host = consumer.get('consumerClientHost', '')
        
        # Tạo key để check trùng
        key = (topic_name, consumer_group_id, consumer_client_id, consumer_client_host)
        
        if key not in seen:
            seen.add(key)
            unique_consumers.append(consumer)
    
    return unique_consumers


def export_consumers_to_csv(consumers: List[Dict[str, Any]], file_path: str) -> None:
    """Ghi danh sách consumers ra file CSV với chỉ 4 cột."""
    if not consumers:
        logger.warning("Không có consumer nào để export.")
        return

    # Loại bỏ trùng lặp
    unique_consumers = remove_duplicate_consumers(consumers)
    logger.info(f"  Đã loại bỏ {len(consumers) - len(unique_consumers)} record(s) trùng lặp.")

    # Chỉ export 4 cột theo yêu cầu
    fieldnames = [
        "topicName",
        "consumerGroupId",
        "consumerClientId",
        "consumerClientHost",
    ]

    with open(file_path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in unique_consumers:
            # Chỉ lấy 4 trường cần thiết
            row = {
                'topicName': c.get('topicName', ''),
                'consumerGroupId': c.get('consumerGroupId', ''),
                'consumerClientId': c.get('consumerClientId', ''),
                'consumerClientHost': c.get('consumerClientHost', ''),
            }
            writer.writerow(normalize_row(row, fieldnames))

    logger.info(f"✅ Đã export {len(unique_consumers)} consumer records (unique) ra file: {file_path}")


def export_consumers_for_cluster(
    client: KafkaUIClient,
    cluster_name: str,
    export_dir: str
) -> None:
    """
    Export tất cả thông tin consumers của các topics trong cluster ra CSV.
    
    Args:
        client: KafkaUIClient instance
        cluster_name: Tên cluster cần export
        export_dir: Thư mục export
    """
    try:
        logger.info(f"📋 Đang lấy TẤT CẢ topics từ cluster '{cluster_name}'...")
        topics = client.get_topics(cluster_name)
        
        if not topics:
            logger.warning(f"⚠️  Cluster '{cluster_name}' không có topic nào.")
            return
        
        logger.info(f"✅ Lấy được {len(topics)} topics từ cluster '{cluster_name}'.")
        
        # Lấy thông tin consumers cho tất cả topics
        logger.info("📊 Đang lấy thông tin consumers cho từng topic...")
        topic_consumers_map = get_all_consumers_for_cluster(client, topics, cluster_name)
        
        # Tạo danh sách tất cả consumers
        all_consumers = []
        topics_with_consumers = 0
        
        for topic in topics:
            topic_name = topic.get('name')
            if not topic_name:
                continue
            
            consumers = topic_consumers_map.get(topic_name, [])
            if consumers:
                all_consumers.extend(consumers)
                topics_with_consumers += 1
                logger.info(f"  Topic '{topic_name}': {len(consumers)} consumer record(s)")
            else:
                # Nếu topic không có consumer, vẫn tạo một record với consumer info rỗng
                empty_consumer_record = {
                    'topicName': topic_name,
                    'consumerGroupId': '',
                    'consumerClientId': '',
                    'consumerClientHost': '',
                }
                all_consumers.append(empty_consumer_record)
                logger.debug(f"  Topic '{topic_name}': Không có consumer (tạo record rỗng)")
        
        logger.info(f"✅ Đã xử lý {len(topics)} topics (có consumers: {topics_with_consumers}, không có consumers: {len(topics) - topics_with_consumers}).")
        
        # Luôn export file CSV, kể cả khi không có consumer
        if not all_consumers:
            logger.warning(f"⚠️  Không có topic nào để export.")
            return
        
        # Tạo tên file với timestamp
        safe_cluster_name = sanitize_name_for_filename(cluster_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(export_dir, f"topic_{safe_cluster_name}_consumer_{timestamp}.csv")
        
        export_consumers_to_csv(all_consumers, output_file)
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi export consumers từ cluster '{cluster_name}'", exc_info=True)
        logger.error(f"Traceback:\n{traceback.format_exc()}")


def main():
    """Main: gọi Kafka UI và export consumer information ra CSV."""
    parser = argparse.ArgumentParser(
        description="Export thông tin consumers từ các topics trong cluster(s) ra CSV"
    )
    parser.add_argument(
        "--cluster",
        type=str,
        default=None,
        help="Tên cluster cần export (nếu không chỉ định sẽ dùng cluster_name từ config.cfg)"
    )
    parser.add_argument(
        "--all-clusters",
        action="store_true",
        help="Export consumers từ TẤT CẢ clusters"
    )
    
    args = parser.parse_args()
    
    # Cấu hình đọc từ config.cfg / biến môi trường / tham số trong KafkaUIConfig
    config = KafkaUIConfig()
    client = KafkaUIClient(config)
    
    export_dir = ensure_export_dir()
    
    try:
        if args.all_clusters:
            # Export tất cả clusters
            logger.info("🔄 Đang lấy danh sách TẤT CẢ clusters...")
            clusters = client.get_clusters()
            logger.info(f"Tìm thấy {len(clusters)} clusters.")
            
            for cluster in clusters:
                cluster_name = cluster.get('name')
                if cluster_name:
                    export_consumers_for_cluster(client, cluster_name, export_dir)
        else:
            # Export cluster được chỉ định
            cluster_name = args.cluster or config.cluster_name
            if not cluster_name:
                logger.error("❌ Không có cluster nào được chỉ định!")
                logger.info("💡 Sử dụng: --cluster <tên_cluster> hoặc --all-clusters")
                return
            
            export_consumers_for_cluster(client, cluster_name, export_dir)
            
    except Exception as e:
        logger.error(f"❌ Lỗi chính: {e}", exc_info=True)
        logger.error(f"Traceback đầy đủ:\n{traceback.format_exc()}")
        logger.info("\n💡 Gợi ý:")
        logger.info("  1. Đảm bảo Kafka UI đang chạy và cấu hình đúng trong config.cfg")
        logger.info("  2. Kiểm tra SESSION cookie hoặc username/password có đúng không")
        logger.info("  3. Kiểm tra tên cluster có đúng không")
        logger.info("  4. Thử: python main_list_topic_consumer.py --all-clusters")
        sys.exit(1)


if __name__ == "__main__":
    main()

