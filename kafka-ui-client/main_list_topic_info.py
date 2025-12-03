"""
Liệt kê toàn bộ thông tin các topic trong một cluster và ghi ra file CSV.
Có thể chỉ định cluster từ command line hoặc export tất cả clusters.
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


def get_topic_message_count_from_data(topic_data: Dict[str, Any]) -> Optional[int]:
    """
    Lấy tổng số messages của một topic từ dữ liệu topic đã có (không cần gọi API thêm).
    
    Cách lấy:
    1. Response từ get_topics() đã có field 'partitions' - danh sách các partitions
    2. Mỗi partition có 'offsetMax' (high watermark) và 'offsetMin' (low watermark)
    3. Số messages trong mỗi partition = offsetMax - offsetMin
    4. Tổng số messages trong topic = tổng (offsetMax - offsetMin) của tất cả partitions
    
    Giải thích:
    - offsetMax: offset cao nhất (high watermark) - offset của message cuối cùng + 1
    - offsetMin: offset thấp nhất (low watermark) - offset của message đầu tiên còn tồn tại
    - offsetMax - offsetMin = số lượng messages thực tế trong partition (sau khi trừ các message đã bị xóa/compact)
    
    Args:
        topic_data: Dict chứa thông tin topic từ get_topics() response
        
    Returns:
        Tổng số messages hoặc None nếu không lấy được
    """
    try:
        partitions = topic_data.get('partitions', [])
        
        if not partitions:
            return None
        
        total_messages = 0
        for partition in partitions:
            offset_max = partition.get('offsetMax')
            offset_min = partition.get('offsetMin')
            
            # Kiểm tra cả offsetMax và offsetMin đều có giá trị hợp lệ
            if (offset_max is not None and isinstance(offset_max, (int, float)) and
                offset_min is not None and isinstance(offset_min, (int, float))):
                # Số messages trong partition = offsetMax - offsetMin
                partition_messages = int(offset_max) - int(offset_min)
                if partition_messages > 0:
                    total_messages += partition_messages
        
        return total_messages if total_messages > 0 else None
    except Exception as e:
        topic_name = topic_data.get('name', 'unknown')
        logger.warning(f"Không thể lấy số lượng messages cho topic '{topic_name}': {e}")
        return None


def export_topics_to_csv(topics: List[Dict[str, Any]], file_path: str) -> None:
    """Ghi danh sách topics ra file CSV."""
    if not topics:
        logger.warning("Không có topic nào để export.")
        return

    # Loại bỏ trường 'partitions' khỏi dữ liệu trước khi export
    topics_clean = []
    for t in topics:
        topic_copy = t.copy()
        topic_copy.pop('partitions', None)  # Bỏ trường partitions nếu có
        topics_clean.append(topic_copy)

    # Lấy tập hợp tất cả key xuất hiện trong các topic (đảm bảo đủ cột)
    fieldnames_set = set()
    for t in topics_clean:
        fieldnames_set.update(t.keys())

    # Sắp xếp cột, ưu tiên một số cột hay dùng lên đầu
    preferred_order = [
        "name",
        "totalMessages",  # Thêm cột số lượng messages
        "internal",
        "partitionCount",
        "replicationFactor",
        "replicas",
        "inSyncReplicas",
        "segmentSize",
        "segmentCount",
        "bytesInPerSec",
        "bytesOutPerSec",
        "underReplicatedPartitions",
        "cleanUpPolicy",
    ]
    remaining = [f for f in sorted(fieldnames_set) if f not in preferred_order]
    fieldnames = [f for f in preferred_order if f in fieldnames_set] + remaining

    with open(file_path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in topics_clean:
            writer.writerow(normalize_row(t, fieldnames))

    logger.info(f"✅ Đã export {len(topics)} topics ra file: {file_path}")


def export_topics_for_cluster(
    client: KafkaUIClient,
    cluster_name: str,
    export_dir: str
) -> None:
    """
    Export tất cả topics của một cluster ra CSV, bao gồm số lượng messages.
    
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
        
        # Thêm thông tin số lượng messages vào mỗi topic (từ dữ liệu đã có, không cần gọi API thêm)
        logger.info("📊 Đang tính số lượng messages cho từng topic từ dữ liệu đã có...")
        for i, topic in enumerate(topics, 1):
            topic_name = topic.get('name')
            if topic_name:
                message_count = get_topic_message_count_from_data(topic)
                if message_count is not None:
                    topic['totalMessages'] = message_count
                    logger.info(f"  [{i}/{len(topics)}] Topic '{topic_name}': {message_count:,} messages")
                else:
                    topic['totalMessages'] = None
                    logger.warning(f"  [{i}/{len(topics)}] Topic '{topic_name}': Không thể tính số lượng messages")
        
        safe_cluster_name = sanitize_name_for_filename(cluster_name)
        # Thêm timestamp vào tên file: format YYYYMMDD_HHMMSS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(export_dir, f"topic_{safe_cluster_name}_info_{timestamp}.csv")
        export_topics_to_csv(topics, output_file)
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi export topics từ cluster '{cluster_name}'", exc_info=True)
        logger.error(f"Traceback:\n{traceback.format_exc()}")


def main():
    """Main: gọi Kafka UI và export topics ra CSV."""
    parser = argparse.ArgumentParser(
        description="Export tất cả topics từ cluster(s) ra CSV"
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
        help="Export topics từ TẤT CẢ clusters"
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
                    export_topics_for_cluster(client, cluster_name, export_dir)
        else:
            # Export cluster được chỉ định
            cluster_name = args.cluster or config.cluster_name
            if not cluster_name:
                logger.error("❌ Không có cluster nào được chỉ định!")
                logger.info("💡 Sử dụng: --cluster <tên_cluster> hoặc --all-clusters")
                return
            
            export_topics_for_cluster(client, cluster_name, export_dir)
            
    except Exception as e:
        logger.error(f"❌ Lỗi chính: {e}", exc_info=True)
        logger.error(f"Traceback đầy đủ:\n{traceback.format_exc()}")
        logger.info("\n💡 Gợi ý:")
        logger.info("  1. Đảm bảo Kafka UI đang chạy và cấu hình đúng trong config.cfg")
        logger.info("  2. Kiểm tra SESSION cookie hoặc username/password có đúng không")
        logger.info("  3. Kiểm tra tên cluster có đúng không")
        logger.info("  4. Thử: python main_list_topic_info.py --all-clusters")
        sys.exit(1)


if __name__ == "__main__":
    main()


