"""
Rà soát toàn bộ các topic có trong cluster chỉ định và lấy ra 20 thông tin bản tin mới nhất 
trong topic đó, lưu vào folder export-msgs_<timestamp>. Mỗi topic tạo thành 1 file JSON array với 
tên file: <Cluster_name>_<Topic_name>.json
"""

import argparse
import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import sys
import os
# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.kafka_ui_client import KafkaUIClient
from config import KafkaUIConfig

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'kafka_export.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def ensure_export_msgs_dir() -> str:
    """
    Đảm bảo tồn tại thư mục export-msgs_<timestamp> ở root project, trả về đường dẫn tuyệt đối.
    - Mỗi lần chạy sẽ tạo một folder mới theo timestamp: YYYYMMDD_HHMMSS
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_dir = os.path.join(base_dir, f"export-msgs_{timestamp}")
    os.makedirs(export_dir, exist_ok=True)
    return export_dir


def sanitize_name_for_filename(name: str) -> str:
    """Chuẩn hóa tên để dùng trong tên file: chỉ giữ [A-Za-z0-9_.-], còn lại thay bằng '_'."""
    if not name:
        return "default"
    # Thay mọi chuỗi ký tự không hợp lệ bằng dấu gạch dưới
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name.strip())
    return safe or "default"


def get_latest_messages_from_topic(
    client: KafkaUIClient,
    topic_name: str,
    cluster_name: str,
    limit: int = 20
) -> List[Dict[str, Any]]:
    """
    Lấy các message mới nhất từ một topic.
    
    Args:
        client: KafkaUIClient instance
        topic_name: Tên topic
        cluster_name: Tên cluster
        limit: Số lượng messages tối đa (mặc định 20)
        
    Returns:
        List các messages (mới nhất)
    """
    try:
        # Lấy messages từ topic (không dùng seekType để tránh lỗi 400)
        logger.info(f"    🔄 Gọi API lấy {limit} message(s) từ topic '{topic_name}'...")
        messages = client.get_topic_messages(
            topic_name=topic_name,
            cluster_name=cluster_name,
            limit=limit
        )
        
        if messages:
            logger.info(f"    ✅ Nhận được {len(messages)} message(s) từ topic '{topic_name}'")
        else:
            logger.warning(f"    ⚠️  Không nhận được message nào từ topic '{topic_name}'")
        
        return messages if messages else []
        
    except Exception as e:
        logger.error(f"    ❌ Lỗi khi lấy messages từ topic '{topic_name}': {e}",exc_info=True)
        logger.debug(f"    Traceback: {traceback.format_exc()}")
        return []


def export_messages_to_json(
    messages: List[Dict[str, Any]],
    file_path: str
) -> None:
    """
    Ghi danh sách messages ra file JSON array, chỉ giữ lại các trường quan trọng:
    - key
    - headers
    - content (hoặc value nếu không có content)
    
    Args:
        messages: Danh sách messages gốc từ Kafka UI
        file_path: Đường dẫn file output
    """
    try:
        simplified_messages: List[Dict[str, Any]] = []
        
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            
            key = msg.get("key")
            headers = msg.get("headers")
            content = msg.get("content", msg.get("value"))

            # Bỏ qua record hoàn toàn rỗng (không có key, headers, content)
            if key is None and (not headers) and content is None:
                continue

            simplified_messages.append(
                {
                    "key": key,
                    "headers": headers or {},
                    "content": content,
                }
            )

        with open(file_path, mode="w", encoding="utf-8") as f:
            json.dump(simplified_messages, f, ensure_ascii=False, indent=2)
        logger.info(
            f"    ✅ Đã lưu {len(simplified_messages)} message(s) (đã rút gọn) vào: {os.path.basename(file_path)}"
        )
    except Exception as e:
        logger.error(f"    ❌ Lỗi khi ghi file '{file_path}': {e}")


def extract_messages_for_cluster(
    client: KafkaUIClient,
    cluster_name: str,
    export_dir: str,
    message_limit: int = 20
) -> None:
    """
    Rà soát tất cả topics trong cluster và lấy 20 message mới nhất từ mỗi topic.
    
    Args:
        client: KafkaUIClient instance
        cluster_name: Tên cluster cần xử lý
        export_dir: Thư mục export
        message_limit: Số lượng messages tối đa cho mỗi topic (mặc định 20)
    """
    try:
        logger.info(f"📋 Đang lấy TẤT CẢ topics từ cluster '{cluster_name}'...")
        topics = client.get_topics(cluster_name)
        
        if not topics:
            logger.warning(f"⚠️  Cluster '{cluster_name}' không có topic nào.")
            return
        
        logger.info(f"✅ Lấy được {len(topics)} topics từ cluster '{cluster_name}'.")
        
        safe_cluster_name = sanitize_name_for_filename(cluster_name)
        successful_exports = 0
        failed_exports = 0
        empty_topics = 0
        
        # Xử lý từng topic
        for idx, topic in enumerate(topics, 1):
            topic_name = topic.get('name')
            if not topic_name:
                logger.warning(f"  [{idx}/{len(topics)}] Topic không có tên, bỏ qua.")
                continue
            
            try:
                logger.info(f"  [{idx}/{len(topics)}] Đang lấy {message_limit} message mới nhất từ topic '{topic_name}'...")
                
                # Lấy messages mới nhất
                messages = get_latest_messages_from_topic(
                    client=client,
                    topic_name=topic_name,
                    cluster_name=cluster_name,
                    limit=message_limit
                )
                
                if not messages:
                    logger.warning(f"    ⚠️  Topic '{topic_name}' không có message nào.")
                    empty_topics += 1
                    # Vẫn tạo file JSON với array rỗng
                    safe_topic_name = sanitize_name_for_filename(topic_name)
                    file_name = f"{safe_cluster_name}_{safe_topic_name}.json"
                    file_path = os.path.join(export_dir, file_name)
                    export_messages_to_json([], file_path)
                    continue
                
                # Tạo tên file: <Cluster_name>_<Topic_name>.json
                safe_topic_name = sanitize_name_for_filename(topic_name)
                file_name = f"{safe_cluster_name}_{safe_topic_name}.json"
                file_path = os.path.join(export_dir, file_name)
                
                # Lưu messages ra file JSON
                export_messages_to_json(messages, file_path)
                successful_exports += 1
                
            except Exception as e:
                logger.error(f"  ❌ Lỗi khi xử lý topic '{topic_name}': {e}")
                logger.debug(f"  Traceback: {traceback.format_exc()}")
                failed_exports += 1
                continue
        
        # Tóm tắt kết quả
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"📊 TÓM TẮT KẾT QUẢ:")
        logger.info(f"   ✅ Export thành công: {successful_exports} topic(s)")
        logger.info(f"   ⚠️  Topic không có message: {empty_topics} topic(s)")
        logger.info(f"   ❌ Lỗi: {failed_exports} topic(s)")
        logger.info(f"   📁 Thư mục export: {export_dir}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi extract messages từ cluster '{cluster_name}'", exc_info=True)
        logger.error(f"Traceback:\n{traceback.format_exc()}")


def main():
    """Main: rà soát topics và extract messages ra JSON files."""
    parser = argparse.ArgumentParser(
        description="Rà soát tất cả topics trong cluster và lấy 20 message mới nhất từ mỗi topic, lưu vào export-msgs"
    )
    parser.add_argument(
        "--cluster",
        type=str,
        default=None,
        help="Tên cluster cần xử lý (nếu không chỉ định sẽ dùng cluster_name từ config.cfg)"
    )
    parser.add_argument(
        "--all-clusters",
        action="store_true",
        help="Xử lý TẤT CẢ clusters"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Số lượng messages tối đa cho mỗi topic (mặc định: 20)"
    )
    
    args = parser.parse_args()
    
    # Cấu hình đọc từ config.cfg / biến môi trường / tham số trong KafkaUIConfig
    config = KafkaUIConfig()
    client = KafkaUIClient(config)
    
    export_dir = ensure_export_msgs_dir()
    
    try:
        if args.all_clusters:
            # Xử lý tất cả clusters
            logger.info("🔄 Đang lấy danh sách TẤT CẢ clusters...")
            clusters = client.get_clusters()
            logger.info(f"Tìm thấy {len(clusters)} clusters.")
            
            for cluster in clusters:
                cluster_name = cluster.get('name')
                if cluster_name:
                    logger.info("")
                    logger.info("=" * 60)
                    logger.info(f"🔄 Đang xử lý cluster: '{cluster_name}'")
                    logger.info("=" * 60)
                    extract_messages_for_cluster(client, cluster_name, export_dir, args.limit)
        else:
            # Xử lý cluster được chỉ định
            cluster_name = args.cluster or config.cluster_name
            if not cluster_name:
                logger.error("❌ Không có cluster nào được chỉ định!")
                logger.info("💡 Sử dụng: --cluster <tên_cluster> hoặc --all-clusters")
                return
            
            extract_messages_for_cluster(client, cluster_name, export_dir, args.limit)
            
    except Exception as e:
        logger.error(f"❌ Lỗi chính: {e}", exc_info=True)
        logger.error(f"Traceback đầy đủ:\n{traceback.format_exc()}")
        logger.info("\n💡 Gợi ý:")
        logger.info("  1. Đảm bảo Kafka UI đang chạy và cấu hình đúng trong config.cfg")
        logger.info("  2. Kiểm tra SESSION cookie hoặc username/password có đúng không")
        logger.info("  3. Kiểm tra tên cluster có đúng không")
        logger.info("  4. Thử: python main_extract_msg_from_topics.py --all-clusters")
        sys.exit(1)


if __name__ == "__main__":
    main()

