import psutil
import time
from typing import Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ResourceManager:
    """系统资源管理器"""
    def __init__(self):
        self.cpu_threshold = 90  # CPU使用率阈值
        self.memory_threshold = 90  # 内存使用率阈值
        self.last_check = time.time()
        self.check_interval = 1  # 资源检查间隔（秒）
        
    def get_system_resources(self) -> Dict[str, Any]:
        """获取系统资源使用情况"""
        current_time = time.time()
        
        # 限制检查频率
        if current_time - self.last_check < self.check_interval:
            return {}
            
        self.last_check = current_time
        
        try:
            return {
                'cpu_percent': psutil.cpu_percent(),
                'memory_percent': psutil.virtual_memory().percent,
                'connections': len(psutil.net_connections()),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting system resources: {str(e)}")
            return {}
            
    def check_resources(self) -> tuple[bool, str]:
        """检查系统资源是否超限"""
        resources = self.get_system_resources()
        
        if not resources:
            return True, ""
            
        if resources.get('cpu_percent', 0) > self.cpu_threshold:
            return False, "CPU负载过高"
            
        if resources.get('memory_percent', 0) > self.memory_threshold:
            return False, "内存不足"
            
        return True, ""
        
    def get_available_workers(self) -> int:
        """获取可用的工作线程数"""
        try:
            cpu_count = psutil.cpu_count() or 1
            return min(32, cpu_count * 4)  # 每个CPU最多4个工作线程
        except:
            return 4  # 默认值
            
    def get_memory_limit(self) -> int:
        """获取可用的最大内存限制（字节）"""
        try:
            total_memory = psutil.virtual_memory().total
            return int(total_memory * 0.4)  # 使用40%的系统内存
        except:
            return 2 * 1024 * 1024 * 1024  # 默认2GB
