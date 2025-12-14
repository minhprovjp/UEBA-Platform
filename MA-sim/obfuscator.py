# simulation_v2/obfuscator.py
import random
import re

class SQLObfuscator:
    @staticmethod
    def obfuscate(sql):
        """Biến đổi SQL để né tránh detection cơ bản"""
        strategies = [
            SQLObfuscator.add_comments,
            SQLObfuscator.random_case,
            SQLObfuscator.whitespace_padding,
            SQLObfuscator.hex_encoding,
            SQLObfuscator.char_encoding,      # [NEW]
            SQLObfuscator.version_comment,    # [NEW]
            SQLObfuscator.scientific_notation # [NEW]
        ]
        
        # Hacker giỏi sẽ dùng nhiều kỹ thuật cùng lúc
        num_strategies = random.randint(1, 3)
        chosen = random.sample(strategies, num_strategies)
        
        obs_sql = sql
        for func in chosen:
            obs_sql = func(obs_sql)
            
        return obs_sql

    @staticmethod
    def add_comments(sql):
        # SELECT * -> SELECT/**/ *
        return sql.replace(" ", "/**/" if random.random() < 0.5 else " ")

    @staticmethod
    def random_case(sql):
        # SELECT -> SeLeCt
        return "".join([c.lower() if random.random() < 0.5 else c.upper() for c in sql])

    @staticmethod
    def whitespace_padding(sql):
        # SELECT id -> SELECT    id (Tabs, Newlines)
        replacements = [" ", "\t", "\n", "\r", "   "]
        return sql.replace(" ", random.choice(replacements))
    
    @staticmethod
    def hex_encoding(sql):
        # 'admin' -> 0x61646D696E
        def to_hex(match):
            s = match.group(1)
            # Chỉ encode nếu chuỗi ngắn (tránh làm query quá dài)
            if len(s) < 20:
                return "0x" + s.encode('utf-8').hex()
            return f"'{s}'"
        return re.sub(r"'([^']{2,})'", to_hex, sql)

    @staticmethod
    def char_encoding(sql):
        # [NEW] 'admin' -> CHAR(97,100,109,105,110)
        def to_char(match):
            s = match.group(1)
            if len(s) < 10: # Chỉ dùng cho chuỗi ngắn
                ords = ",".join([str(ord(c)) for c in s])
                return f"CHAR({ords})"
            return f"'{s}'"
        return re.sub(r"'([^']{2,})'", to_char, sql)

    @staticmethod
    def version_comment(sql):
        # [NEW] SELECT -> /*!50000SELECT*/
        # Kỹ thuật này thường dùng để bypass WAF nhưng vẫn chạy trên MySQL
        keywords = ["SELECT", "UNION", "AND", "OR"]
        target = random.choice(keywords)
        if target in sql.upper():
            # Regex replace case-insensitive
            pattern = re.compile(re.escape(target), re.IGNORECASE)
            return pattern.sub(f"/*!50000{target}*/", sql, count=1)
        return sql

    @staticmethod
    def scientific_notation(sql):
        # [NEW] WHERE id = 1 -> WHERE id = 1.0e0
        # Làm rối các bộ lọc dựa trên kiểu dữ liệu
        return re.sub(r"=\s*(\d+)", r"= \1.0e0", sql)