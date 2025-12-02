"""
Cấu hình cho Kafka UI API Client
"""
import os
from typing import Optional

class KafkaUIConfig:
    """Quản lý cấu hình kết nối đến Kafka UI"""
    
    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        username: Optional[str] = "admin",
        password: Optional[str] = "admin",
        session_token: Optional[str] = None,
        cluster_name: str = "local",
        timeout: int = 30
    ):
        """
        Khởi tạo cấu hình Kafka UI
        
        Args:
            base_url: URL của Kafka UI (mặc định: http://localhost:8080)
            username: Tên người dùng (nếu dùng Basic Auth)
            password: Mật khẩu (nếu dùng Basic Auth)
            session_token: Giá trị SESSION (nếu muốn auth qua Cookie SESSION=...)
            cluster_name: Tên cluster Kafka (mặc định: local)
            timeout: Timeout cho requests (giây)
        """
        self.base_url = base_url.rstrip('/')
        self.username = username or os.getenv('KAFKA_UI_USERNAME')
        self.password = password or os.getenv('KAFKA_UI_PASSWORD')
        # Ưu tiên giá trị truyền trực tiếp, nếu không có thì lấy từ biến môi trường
        # Ví dụ: export KAFKA_UI_SESSION=xxxxxxxxxxxx
        self.session_token = session_token or os.getenv('KAFKA_UI_SESSION')
        self.cluster_name = cluster_name
        self.timeout = timeout
        
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

