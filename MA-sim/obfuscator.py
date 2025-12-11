# simulation_v2/obfuscator.py
import random

class SQLObfuscator:
    @staticmethod
    def obfuscate(sql):
        """Biến đổi SQL để né tránh detection cơ bản"""
        strategies = [
            SQLObfuscator.add_comments,
            SQLObfuscator.random_case,
            SQLObfuscator.whitespace_padding,
            SQLObfuscator.hex_encoding
        ]
        
        # Chọn ngẫu nhiên 1 hoặc nhiều kỹ thuật
        num_strategies = random.randint(1, 2)
        chosen = random.sample(strategies, num_strategies)
        
        obs_sql = sql
        for func in chosen:
            obs_sql = func(obs_sql)
            
        return obs_sql

    @staticmethod
    def add_comments(sql):
        # SELECT * FROM -> SELECT/**/ * /**/FROM
        return sql.replace(" ", "/**/")

    @staticmethod
    def random_case(sql):
        # SELECT -> SeLeCt
        return "".join([c.lower() if random.random() < 0.5 else c.upper() for c in sql])

    @staticmethod
    def whitespace_padding(sql):
        # SELECT id -> SELECT    id
        return sql.replace(" ", " " * random.randint(2, 5))
    
    @staticmethod
    def hex_encoding(sql):
        # Chỉ encode các chuỗi trong ngoặc đơn, ví dụ 'admin'
        # Tìm các chuỗi 'string'
        import re
        def to_hex(match):
            s = match.group(1)
            # 0x...
            return "0x" + s.encode('utf-8').hex()
            
        return re.sub(r"'([^']{2,})'", to_hex, sql)