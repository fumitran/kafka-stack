"""
Kafka UI API Client - Kéo thông tin từ Kafka UI
"""
import requests
from typing import Dict, List, Optional, Any
import sys
import os
# Add src directory to path to import config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import KafkaUIConfig


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
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
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
        all_topics = []
        
        # Lấy page đầu tiên để kiểm tra pagination
        response = self._make_request('GET', f'/api/clusters/{cluster}/topics')
        
        # Kiểm tra xem response có pagination không
        if isinstance(response, dict) and 'pageCount' in response:
            page_count = response.get('pageCount', 1)
            topics = response.get('topics', [])
            all_topics.extend(topics)
            
            # Nếu có nhiều hơn 1 page, quét tất cả các pages còn lại
            if page_count > 1:
                for page in range(2, page_count + 1):
                    try:
                        page_response = self._make_request(
                            'GET', 
                            f'/api/clusters/{cluster}/topics',
                            params={'page': page}
                        )
                        if isinstance(page_response, dict) and 'topics' in page_response:
                            all_topics.extend(page_response.get('topics', []))
                    except Exception as e:
                        # Log warning nhưng tiếp tục với các pages khác
                        print(f"Warning: Không thể lấy page {page}: {e}")
                        continue
            
            return all_topics
        
        # Nếu là list trực tiếp thì trả về luôn
        if isinstance(response, list):
            return response
        
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
        seek_type: str = 'BEGINNING',
        offset: Optional[int] = None
    ) -> List[Dict]:
        """
        Lấy messages từ topic
        
        Args:
            topic_name: Tên topic
            cluster_name: Tên cluster
            partition: Partition number (None = tất cả partitions)
            limit: Số lượng messages tối đa
            seek_type: BEGINNING, END, OFFSET, TIMESTAMP
            offset: Offset để bắt đầu (nếu seek_type = OFFSET)
        """
        cluster = cluster_name or self.cluster_name
        params = {
            'limit': limit,
            'seekType': seek_type
        }
        
        if partition is not None:
            params['partition'] = partition
        if offset is not None:
            params['offset'] = offset
            
        response = self._make_request(
            'GET',
            f'/api/clusters/{cluster}/topics/{topic_name}/messages',
            params=params
        )

        # Tùy version Kafka UI, API có thể trả về list trực tiếp
        if isinstance(response, list):
            return response
        # Hoặc bọc trong dict với key 'messages'
        if isinstance(response, dict) and 'messages' in response:
            data = response['messages']
            return data if isinstance(data, list) else []
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

