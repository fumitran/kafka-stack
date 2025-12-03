"""
Liệt kê toàn bộ thông tin clusters từ Kafka UI và ghi ra file CSV.
"""

import csv
import json
import os
from typing import Any, Dict, List

import sys
import os
# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.kafka_ui_client import KafkaUIClient
from config import KafkaUIConfig


def ensure_export_dir() -> str:
    """Đảm bảo tồn tại thư mục export, trả về đường dẫn tuyệt đối."""
    # Export directory ở root của project, không phải trong src/
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    export_dir = os.path.join(base_dir, "export")
    os.makedirs(export_dir, exist_ok=True)
    return export_dir


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


def export_clusters_to_csv(clusters: List[Dict[str, Any]], file_path: str) -> None:
    """Ghi danh sách clusters ra file CSV."""
    if not clusters:
        print("Không có cluster nào để export.")
        return

    # Lấy tập hợp tất cả key xuất hiện trong các cluster (đảm bảo đủ cột)
    fieldnames_set = set()
    for c in clusters:
        fieldnames_set.update(c.keys())

    # Sắp xếp cột, ưu tiên một số cột hay dùng lên đầu
    preferred_order = [
        "name",
        "status",
        "brokerCount",
        "topicCount",
        "onlinePartitionCount",
        "version",
        "readOnly",
    ]
    remaining = [f for f in sorted(fieldnames_set) if f not in preferred_order]
    fieldnames = [f for f in preferred_order if f in fieldnames_set] + remaining

    with open(file_path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in clusters:
            writer.writerow(normalize_row(c, fieldnames))

    print(f"✅ Đã export {len(clusters)} clusters ra file: {file_path}")


def main():
    """Main: gọi Kafka UI và export clusters ra CSV."""
    # Cấu hình:
    #   - Tất cả tham số (base_url, username/password, SESSION...) được cấu hình trong file config.py
    #   - Hoặc qua biến môi trường:
    #       KAFKA_UI_BASE_URL, KAFKA_UI_USERNAME, KAFKA_UI_PASSWORD, KAFKA_UI_SESSION, KAFKA_CLUSTER_NAME, ...
    config = KafkaUIConfig()
    client = KafkaUIClient(config)

    try:
        print("Đang lấy danh sách clusters từ Kafka UI...")
        clusters = client.get_clusters()
        print(f"Lấy được {len(clusters)} clusters.")

        export_dir = ensure_export_dir()
        output_file = os.path.join(export_dir, "cluster.csv")
        export_clusters_to_csv(clusters, output_file)
    except Exception as e:
        print(f"\n❌ Lỗi khi export clusters: {e}")
        print("\n💡 Gợi ý:")
        print("  1. Đảm bảo Kafka UI đang chạy trên http://localhost:8080")
        print("  2. Kiểm tra SESSION cookie hoặc username/password có đúng không")
        print("  3. Thử gọi trực tiếp API: curl http://localhost:8080/api/clusters")


if __name__ == "__main__":
    main()


