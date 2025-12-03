"""
Cấu hình cho Kafka UI API Client

Ưu tiên đọc cấu hình theo thứ tự:
1. File config.cfg (trong cùng thư mục)
2. Tham số truyền vào KafkaUIConfig(...)
3. Biến môi trường (KAFKA_UI_BASE_URL, KAFKA_UI_SESSION, ...)
4. Giá trị mặc định trong code
"""

import configparser
import os
from typing import Optional


class KafkaUIConfig:
    """Quản lý cấu hình kết nối đến Kafka UI"""

    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        session_token: Optional[str] = None,
        cluster_name: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        """
        Khởi tạo cấu hình Kafka UI

        Args:
            base_url: URL của Kafka UI
            username: Tên người dùng (nếu dùng Basic Auth)
            password: Mật khẩu (nếu dùng Basic Auth)
            session_token: Giá trị SESSION (nếu muốn auth qua Cookie SESSION=...)
            cluster_name: Tên cluster Kafka
            timeout: Timeout cho requests (giây)
        """
        # 1. Đọc từ file config.cfg (nếu tồn tại)
        cfg_base_url = None
        cfg_username = None
        cfg_password = None
        cfg_session_token = None
        cfg_cluster_name = None
        cfg_timeout: Optional[int] = None

        cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.cfg")
        if os.path.exists(cfg_path):
            parser = configparser.ConfigParser()
            parser.read(cfg_path, encoding="utf-8")
            if parser.has_section("kafka-ui"):
                section = parser["kafka-ui"]
                cfg_base_url = section.get("base_url") or None
                cfg_username = section.get("username") or None
                cfg_password = section.get("password") or None
                cfg_session_token = section.get("session_token") or None
                cfg_cluster_name = section.get("cluster_name") or None
                try:
                    cfg_timeout_val = section.get("timeout")
                    if cfg_timeout_val:
                        cfg_timeout = int(cfg_timeout_val)
                except (TypeError, ValueError):
                    cfg_timeout = None

        # 2. Ưu tiên: config.cfg > tham số truyền vào > biến môi trường > default
        self.base_url = (
            (cfg_base_url or base_url or os.getenv("KAFKA_UI_BASE_URL") or "http://localhost:8080")
            .rstrip("/")
        )
        self.username = cfg_username or username or os.getenv("KAFKA_UI_USERNAME")
        self.password = cfg_password or password or os.getenv("KAFKA_UI_PASSWORD")
        self.session_token = (
            cfg_session_token or session_token or os.getenv("KAFKA_UI_SESSION")
        )
        self.cluster_name = (
            cfg_cluster_name or cluster_name or os.getenv("KAFKA_CLUSTER_NAME") or "local"
        )
        self.timeout = cfg_timeout or timeout or int(os.getenv("KAFKA_UI_TIMEOUT", "30"))

    def get_auth(self) -> Optional[tuple]:
        """Trả về tuple (username, password) nếu có authentication"""
        if self.username and self.password:
            return (self.username, self.password)
        return None

    def get_headers(self) -> dict:
        """
        Trả về headers mặc định cho mọi request.
        - Nếu có session_token thì thêm Cookie: SESSION=<token>
        """
        headers: dict = {}
        if self.session_token:
            headers["Cookie"] = f"SESSION={self.session_token}"
        return headers
