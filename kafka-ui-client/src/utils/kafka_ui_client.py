"""
Kafka UI API Client - Kéo thông tin từ Kafka UI
"""
import requests
from typing import Dict, List, Optional, Any
import sys
import os
import logging
import json

# Add src directory to path to import config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import KafkaUIConfig

logger = logging.getLogger(__name__)


class KafkaUIClient:
    """Client để tương tác với Kafka UI REST API"""
    
    def __init__(self, config: KafkaUIConfig):
        """
        Khởi tạo Kafka UI Client
        
        Args:
            config: Đối tượng KafkaUIConfig chứa thông tin kết nối
        """
        self.config = config
        self.base_url = config.base_url
        self.auth = config.get_auth()
        self.timeout = config.timeout
        self.cluster_name = config.cluster_name
        self.default_headers = config.get_headers()
        
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None
    ) -> Dict:
        """
        Thực hiện HTTP request đến Kafka UI API
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            json_data: JSON body data
            
        Returns:
            Response JSON data
            
        Raises:
            requests.exceptions.RequestException: Nếu request thất bại
        """
        url = f"{self.base_url}{endpoint}"
        headers = dict(self.default_headers or {})
        
        # Log request info
        if params:
            params_str = "&".join([f"{k}={v}" for k, v in params.items()])
            logger.info(f"🌐 API Request: {method} {url}?{params_str}")
        else:
            logger.info(f"🌐 API Request: {method} {url}")
        if json_data:
            logger.debug(f"   Request body: {json_data}")
        
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=headers if headers else None,
                auth=self.auth,
                timeout=self.timeout
            )
            response.raise_for_status()

            # Thử parse JSON; nếu body không phải JSON (ví dụ HTML / empty, text/event-stream)
            # thì cho phép caller tự handle (đặc biệt với /messages).
            try:
                result = response.json()
            except ValueError:
                body_preview = (response.text or "").strip()
                content_type = response.headers.get('Content-Type') or ''
                logger.error(
                    "   ❌ Không parse được JSON từ response "
                    f"(status={response.status_code}, content_type={content_type}). "
                    f"Body (preview 500 chars): {body_preview[:500]!r}"
                )
                # Nếu là endpoint /messages hoặc content-type text/event-stream
                # thì trả về raw text để hàm get_topic_messages tự parse SSE.
                if endpoint.endswith("/messages") or "/messages" in endpoint or "text/event-stream" in content_type:
                    return response.text or ""
                # Ngược lại trả về dict rỗng
                return {}
            
            # Log response info
            if isinstance(result, list):
                logger.info(f"   ✅ Response: {len(result)} item(s)")
            elif isinstance(result, dict):
                logger.info(
                    "   ✅ Response: dict with keys: "
                    f"{list(result.keys())[:5]}{'...' if len(result.keys()) > 5 else ''}"
                )
            else:
                logger.info(f"   ✅ Response: {type(result).__name__}")
            
            return result
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP Error: {e} | body={e.response.text if e.response else 'No response'}",
                exc_info=True,
            )
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error: {e}", exc_info=True)
            raise
    
    # ========== Cluster Information ==========
    
    def get_clusters(self) -> List[Dict]:
        """Lấy danh sách tất cả các clusters"""
        return self._make_request('GET', '/api/clusters')
    
    def get_cluster_info(self, cluster_name: Optional[str] = None) -> Dict:
        """
        Lấy thông tin chi tiết về cluster
        
        Args:
            cluster_name: Tên cluster (mặc định dùng cluster_name từ config)
        """
        cluster = cluster_name or self.cluster_name
        clusters = self.get_clusters()
        for c in clusters:
            if c.get('name') == cluster:
                return c
        raise ValueError(f"Cluster '{cluster}' not found")
    
    # ========== Topics ==========
    
    def get_topics(self, cluster_name: Optional[str] = None) -> List[Dict]:
        """
        Lấy danh sách tất cả topics (tự động quét tất cả các pages nếu có pagination)
        
        Args:
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        logger.info(f"📋 Đang lấy danh sách topics từ cluster '{cluster}'...")
        all_topics = []
        
        # Lấy page đầu tiên để kiểm tra pagination
        response = self._make_request('GET', f'/api/clusters/{cluster}/topics')
        
        # Kiểm tra xem response có pagination không
        if isinstance(response, dict) and 'pageCount' in response:
            page_count = response.get('pageCount', 1)
            topics = response.get('topics', [])
            all_topics.extend(topics)
            logger.info(f"   📄 Page 1/{page_count}: {len(topics)} topic(s)")
            
            # Nếu có nhiều hơn 1 page, quét tất cả các pages còn lại
            if page_count > 1:
                logger.info(f"   📄 Đang lấy thêm {page_count - 1} page(s)...")
                for page in range(2, page_count + 1):
                    try:
                        page_response = self._make_request(
                            'GET', 
                            f'/api/clusters/{cluster}/topics',
                            params={'page': page}
                        )
                        if isinstance(page_response, dict) and 'topics' in page_response:
                            page_topics = page_response.get('topics', [])
                            all_topics.extend(page_topics)
                            logger.info(f"   📄 Page {page}/{page_count}: {len(page_topics)} topic(s)")
                    except Exception as e:
                        # Log warning nhưng tiếp tục với các pages khác
                        logger.warning(f"   ⚠️  Không thể lấy page {page}: {e}")
                        continue
            
            logger.info(f"   ✅ Tổng cộng: {len(all_topics)} topic(s)")
            return all_topics
        
        # Nếu là list trực tiếp thì trả về luôn
        if isinstance(response, list):
            logger.info(f"   ✅ Tổng cộng: {len(response)} topic(s)")
            return response
        
        logger.warning(f"   ⚠️  Response không đúng định dạng, trả về danh sách rỗng")
        return []
    
    def get_topic_details(
        self, 
        topic_name: str, 
        cluster_name: Optional[str] = None
    ) -> Dict:
        """
        Lấy thông tin chi tiết về một topic
        
        Args:
            topic_name: Tên topic
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request('GET', f'/api/clusters/{cluster}/topics/{topic_name}')
    
    def get_topic_messages(
        self,
        topic_name: str,
        cluster_name: Optional[str] = None,
        partition: Optional[int] = None,
        limit: int = 100,
        seek_type: Optional[str] = None,
        offset: Optional[int] = None
    ) -> List[Dict]:
        """
        Lấy messages từ topic
        
        Args:
            topic_name: Tên topic
            cluster_name: Tên cluster
            partition: Partition number (None = tất cả partitions)
            limit: Số lượng messages tối đa
            seek_type: BEGINNING, END, OFFSET, TIMESTAMP (None = không thêm seekType vào request)
            offset: Offset để bắt đầu (nếu seek_type = OFFSET)
        """
        cluster = cluster_name or self.cluster_name
        logger.info(f"📨 Đang lấy messages từ topic '{topic_name}' (cluster: '{cluster}', limit: {limit})...")
        
        params = {
            'limit': limit
        }
        
        # Chỉ thêm seekType vào params nếu được chỉ định
        if seek_type is not None:
            params['seekType'] = seek_type
        
        if partition is not None:
            params['partition'] = partition
        if offset is not None:
            params['offset'] = offset
            
        response = self._make_request(
            'GET',
            f'/api/clusters/{cluster}/topics/{topic_name}/messages',
            params=params
        )

        # Nếu nhận về raw text (text/event-stream), parse SSE để lấy messages
        if isinstance(response, str):
            logger.info("   🧵 Parsing text/event-stream response cho messages...")
            return self._parse_sse_messages_body(response)

        # Tùy version Kafka UI, API có thể trả về list trực tiếp
        if isinstance(response, list):
            return response
        # Hoặc bọc trong dict với key 'messages'
        if isinstance(response, dict) and 'messages' in response:
            data = response['messages']
            return data if isinstance(data, list) else []
        return []

    def _parse_sse_messages_body(self, body: str) -> List[Dict]:
        """
        Parse nội dung text/event-stream từ Kafka UI /messages thành list messages.
        
        - Mỗi dòng sự kiện có dạng: 'data:{...json...}'
        - Một số event có thể chứa field 'messages' (list), hoặc 'message' đơn lẻ.
        - Nếu không tìm thấy messages rõ ràng, sẽ trả về list các event (dict) thô.
        """
        events: List[Dict[str, Any]] = []
        messages: List[Dict[str, Any]] = []

        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith(":"):
                # Bỏ qua dòng comment / keep-alive
                continue
            if not line.startswith("data:"):
                continue

            data_str = line[len("data:"):].strip()
            if not data_str:
                continue

            try:
                evt = json.loads(data_str)
                if isinstance(evt, dict):
                    events.append(evt)
            except Exception as e:
                logger.debug(f"   ⚠️  Không parse được dòng SSE: {data_str!r} ({e})")
                continue

        # Ưu tiên field 'messages' (list) trong event
        for evt in events:
            if not isinstance(evt, dict):
                continue

            # Một số version có thể có field 'messages'
            if 'messages' in evt and isinstance(evt['messages'], list):
                for m in evt['messages']:
                    if isinstance(m, dict):
                        messages.append(m)
                    else:
                        messages.append({'value': m})
                continue

            # Hoặc field 'message' đơn lẻ
            if 'message' in evt:
                m = evt['message']
                if isinstance(m, dict):
                    messages.append(m)
                else:
                    messages.append({'value': m})

        if messages:
            logger.info(f"   ✅ Parsed {len(messages)} message(s) từ SSE")
            return messages

        # Nếu không trích được messages, trả về toàn bộ events để caller có thêm thông tin
        if events:
            logger.info(f"   ⚠️  Không tìm thấy field 'messages', trả về {len(events)} event(s) thô")
            return events

        logger.info("   ⚠️  Không parse được bất kỳ event nào từ SSE, trả về list rỗng")
        return []
    
    def get_topic_config(
        self,
        topic_name: str,
        cluster_name: Optional[str] = None
    ) -> List[Dict]:
        """
        Lấy cấu hình của topic
        
        Args:
            topic_name: Tên topic
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request(
            'GET',
            f'/api/clusters/{cluster}/topics/{topic_name}/config'
        )
    
    # ========== Brokers ==========
    
    def get_brokers(self, cluster_name: Optional[str] = None) -> List[Dict]:
        """
        Lấy danh sách brokers
        
        Args:
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request('GET', f'/api/clusters/{cluster}/brokers')
    
    def get_broker_details(
        self,
        broker_id: int,
        cluster_name: Optional[str] = None
    ) -> Dict:
        """
        Lấy thông tin chi tiết về một broker
        
        Args:
            broker_id: ID của broker
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request(
            'GET',
            f'/api/clusters/{cluster}/brokers/{broker_id}'
        )
    
    # ========== Consumer Groups ==========
    
    def get_consumer_groups(
        self,
        cluster_name: Optional[str] = None
    ) -> List[Dict]:
        """
        Lấy danh sách consumer groups
        
        Args:
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request(
            'GET',
            f'/api/clusters/{cluster}/consumer-groups'
        )
    
    def get_consumer_group_details(
        self,
        consumer_group_id: str,
        cluster_name: Optional[str] = None
    ) -> Dict:
        """
        Lấy thông tin chi tiết về consumer group
        
        Args:
            consumer_group_id: ID của consumer group
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request(
            'GET',
            f'/api/clusters/{cluster}/consumer-groups/{consumer_group_id}'
        )
    
    def get_consumer_group_offsets(
        self,
        consumer_group_id: str,
        cluster_name: Optional[str] = None
    ) -> Dict:
        """
        Lấy thông tin offsets của consumer group
        
        Args:
            consumer_group_id: ID của consumer group
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request(
            'GET',
            f'/api/clusters/{cluster}/consumer-groups/{consumer_group_id}/offsets'
        )
    
    def get_topic_consumer_groups(
        self,
        topic_name: str,
        cluster_name: Optional[str] = None
    ) -> List[Dict]:
        """
        Lấy danh sách consumer groups đang consume một topic cụ thể
        
        Args:
            topic_name: Tên topic
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request(
            'GET',
            f'/api/clusters/{cluster}/topics/{topic_name}/consumer-groups'
        )
    
    # ========== Schemas ==========
    
    def get_schemas(self, cluster_name: Optional[str] = None) -> List[Dict]:
        """
        Lấy danh sách schemas (nếu có Schema Registry)
        
        Args:
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request('GET', f'/api/clusters/{cluster}/schemas')
    
    # ========== Metrics ==========
    
    def get_metrics(self, cluster_name: Optional[str] = None) -> Dict:
        """
        Lấy metrics của cluster
        
        Args:
            cluster_name: Tên cluster
        """
        cluster = cluster_name or self.cluster_name
        return self._make_request('GET', f'/api/clusters/{cluster}/metrics')
    
    # ========== Utility Methods ==========
    
    def search_topics(
        self,
        search_term: str,
        cluster_name: Optional[str] = None
    ) -> List[Dict]:
        """
        Tìm kiếm topics theo tên
        
        Args:
            search_term: Từ khóa tìm kiếm
            cluster_name: Tên cluster
        """
        all_topics = self.get_topics(cluster_name)
        search_term_lower = search_term.lower()
        return [
            topic for topic in all_topics
            if search_term_lower in topic.get('name', '').lower()
        ]
    
    def get_topic_statistics(
        self,
        topic_name: str,
        cluster_name: Optional[str] = None
    ) -> Dict:
        """
        Lấy thống kê tổng hợp về topic
        
        Args:
            topic_name: Tên topic
            cluster_name: Tên cluster
        """
        topic_details = self.get_topic_details(topic_name, cluster_name)

        partitions = topic_details.get('partitions') or []
        stats = {
            'name': topic_details.get('name'),
            'partitions': len(partitions),
            'total_messages': 0,
            'total_size': 0,
            'replication_factor': 0,
        }

        for partition in partitions:
            # Một số field có thể là null từ API → dùng {} / [] mặc định
            leader = partition.get('leader') or {}
            size = partition.get('size') or {}
            replicas = partition.get('replicas') or []

            stats['total_messages'] += leader.get('offset', 0)
            stats['total_size'] += size.get('value', 0)
            stats['replication_factor'] = max(
                stats['replication_factor'],
                len(replicas),
            )

        return stats

