"""
Liệt kê toàn bộ thông tin các topic trong một cluster và ghi ra file CSV.
"""

import csv
import json
import os
import re
from typing import Any, Dict, List

from kafka_ui_client import KafkaUIClient
from config import KafkaUIConfig


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


def export_topics_to_csv(topics: List[Dict[str, Any]], file_path: str) -> None:
    """Ghi danh sách topics ra file CSV."""
    if not topics:
        print("Không có topic nào để export.")
        return

    # Lấy tập hợp tất cả key xuất hiện trong các topic (đảm bảo đủ cột)
    fieldnames_set = set()
    for t in topics:
        fieldnames_set.update(t.keys())

    # Sắp xếp cột, ưu tiên một số cột hay dùng lên đầu
    preferred_order = [
        "name",
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
        for t in topics:
            writer.writerow(normalize_row(t, fieldnames))

    print(f"✅ Đã export {len(topics)} topics ra file: {file_path}")


def main():
    """Main: gọi Kafka UI và export topics ra CSV."""
    # Cấu hình đọc từ config.cfg / biến môi trường / tham số trong KafkaUIConfig
    config = KafkaUIConfig()
    client = KafkaUIClient(config)

    cluster_name = config.cluster_name

    try:
        print(f"Đang lấy danh sách topics từ cluster '{cluster_name}'...")
        topics = client.get_topics(cluster_name)
        print(f"Lấy được {len(topics)} topics.")

        export_dir = ensure_export_dir()
        safe_cluster_name = sanitize_name_for_filename(cluster_name)
        output_file = os.path.join(export_dir, f"topic_{safe_cluster_name}_info.csv")
        export_topics_to_csv(topics, output_file)
    except Exception as e:
        print(f"\n❌ Lỗi khi export topics: {e}")
        print("\n💡 Gợi ý:")
        print("  1. Đảm bảo Kafka UI đang chạy và cấu hình cluster đúng trong config.cfg")
        print("  2. Kiểm tra SESSION cookie hoặc username/password có đúng không")
        print(f"  3. Thử gọi trực tiếp API: curl http://localhost:8080/api/clusters/{cluster_name}/topics")


if __name__ == "__main__":
    main()


