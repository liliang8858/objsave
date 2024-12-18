import jwt
import bcrypt
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import secrets
from threading import Lock
import re

logger = logging.getLogger(__name__)

class SecurityManager:
    """
    安全管理器
    - JWT认证
    - 密码加密
    - API密钥管理
    - 权限控制
    - 请求验证
    """
    def __init__(self, secret_key: str = None):
        self.secret_key = secret_key or secrets.token_urlsafe(32)
        self.api_keys: Dict[str, Dict] = {}
        self.api_key_lock = Lock()
        
        # 权限配置
        self.roles = {
            "admin": ["read", "write", "delete", "manage"],
            "user": ["read", "write"],
            "readonly": ["read"]
        }
        
        # 安全统计
        self.stats = {
            "failed_logins": 0,
            "invalid_tokens": 0,
            "blocked_requests": 0
        }
        self.stats_lock = Lock()
        
    def hash_password(self, password: str) -> str:
        """使用bcrypt加密密码"""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode(), salt).decode()
        
    def verify_password(self, password: str, hashed: str) -> bool:
        """验证密码"""
        try:
            return bcrypt.checkpw(password.encode(), hashed.encode())
        except Exception as e:
            logger.error(f"Password verification error: {str(e)}")
            return False
            
    def create_token(self, user_id: str, role: str = "user", expires_in: int = 3600) -> str:
        """创建JWT令牌"""
        try:
            payload = {
                "user_id": user_id,
                "role": role,
                "permissions": self.roles.get(role, []),
                "exp": datetime.utcnow() + timedelta(seconds=expires_in),
                "iat": datetime.utcnow()
            }
            return jwt.encode(payload, self.secret_key, algorithm="HS256")
        except Exception as e:
            logger.error(f"Token creation error: {str(e)}")
            raise
            
    def verify_token(self, token: str) -> Optional[Dict]:
        """验证JWT令牌"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=["HS256"])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token has expired")
            with self.stats_lock:
                self.stats["invalid_tokens"] += 1
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {str(e)}")
            with self.stats_lock:
                self.stats["invalid_tokens"] += 1
            return None
            
    def create_api_key(self, user_id: str, role: str = "user") -> str:
        """创建API密钥"""
        api_key = secrets.token_urlsafe(32)
        with self.api_key_lock:
            self.api_keys[api_key] = {
                "user_id": user_id,
                "role": role,
                "created_at": datetime.utcnow().isoformat(),
                "last_used": None
            }
        return api_key
        
    def verify_api_key(self, api_key: str) -> Optional[Dict]:
        """验证API密钥"""
        with self.api_key_lock:
            if api_key in self.api_keys:
                self.api_keys[api_key]["last_used"] = datetime.utcnow().isoformat()
                return self.api_keys[api_key]
        return None
        
    def has_permission(self, token_or_key: str, required_permission: str) -> bool:
        """检查是否有权限"""
        try:
            # 先尝试作为JWT令牌验证
            payload = self.verify_token(token_or_key)
            if payload:
                return required_permission in payload.get("permissions", [])
                
            # 再尝试作为API密钥验证
            api_key_info = self.verify_api_key(token_or_key)
            if api_key_info:
                role = api_key_info.get("role")
                return required_permission in self.roles.get(role, [])
                
            return False
        except Exception as e:
            logger.error(f"Permission check error: {str(e)}")
            return False
            
    def validate_request_data(self, data: Dict) -> bool:
        """验证请求数据安全性"""
        try:
            # 检查SQL注入
            sql_pattern = r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|UNION|ALTER)\b)"
            for value in str(data).split():
                if re.search(sql_pattern, value, re.IGNORECASE):
                    logger.warning(f"Potential SQL injection detected: {value}")
                    with self.stats_lock:
                        self.stats["blocked_requests"] += 1
                    return False
                    
            # 检查XSS攻击
            xss_pattern = r"<script|javascript:|data:|vbscript:"
            for value in str(data).split():
                if re.search(xss_pattern, value, re.IGNORECASE):
                    logger.warning(f"Potential XSS attack detected: {value}")
                    with self.stats_lock:
                        self.stats["blocked_requests"] += 1
                    return False
                    
            return True
        except Exception as e:
            logger.error(f"Request validation error: {str(e)}")
            return False
            
    def get_stats(self) -> Dict[str, int]:
        """获取安全统计信息"""
        with self.stats_lock:
            return {
                "failed_logins": self.stats["failed_logins"],
                "invalid_tokens": self.stats["invalid_tokens"],
                "blocked_requests": self.stats["blocked_requests"],
                "active_api_keys": len(self.api_keys)
            }
