# simulation_v2/stats_utils.py
import numpy as np
import random

class StatisticalGenerator:
    """
    Bộ sinh số ngẫu nhiên nâng cao mô phỏng hành vi con người.
    """
    
    @staticmethod
    def pick_zipfian_item(items, alpha=1.2):
        """
        Chọn một phần tử trong danh sách theo luật Zipf.
        Các phần tử đầu danh sách có xác suất chọn cao hơn nhiều so với cuối.
        
        Args:
            items (list): Danh sách dữ liệu nguồn.
            alpha (float): Tham số độ dốc (thường 1.1 - 1.5). Càng cao càng chênh lệch.
        """
        if not items: return None
        n = len(items)
        if n == 1: return items[0]
        
        # Sinh ra một rank từ phân phối Zipf (1, 2, 3...)
        # Rank càng nhỏ xác suất càng cao
        rank = np.random.zipf(alpha)
        
        # Map rank vào index của list (sử dụng modulo để tránh out of bound)
        # Trừ 1 vì rank bắt đầu từ 1, index bắt đầu từ 0
        index = (rank - 1) % n
        
        return items[index]

    @staticmethod
    def generate_pareto_delay(min_delay=1, mode_delay=5, alpha=1.5):
        """
        Sinh thời gian chờ (Think Time) theo phân phối Pareto (Bursty behavior).
        Phần lớn thời gian chờ sẽ ngắn (quanh mode_delay), nhưng thỉnh thoảng sẽ có 
        thời gian chờ rất dài (Long tail).
        
        Args:
            min_delay (float): Thời gian chờ tối thiểu (giây).
            mode_delay (float): Thời gian chờ phổ biến nhất.
            alpha (float): Shape parameter (thường 1.5 - 2.0). Càng nhỏ đuôi càng dài.
        """
        # Pareto chuẩn bắt đầu từ 1. Ta scale nó lên.
        # Công thức: (np.random.pareto(alpha) + 1) * scale
        
        # Tính scale sao cho giá trị kỳ vọng hợp lý
        raw_pareto = np.random.pareto(alpha)
        delay = (raw_pareto * mode_delay) + min_delay
        
        # Cắt trần (Cap) để tránh treo thread quá lâu (ví dụ max 1 tiếng ảo)
        # Trong mô phỏng tăng tốc, cần lưu ý đơn vị thời gian.
        return delay

# Test nhanh
if __name__ == "__main__":
    items = ["Product A (Hot)", "Product B", "Product C", "Product D", "Product E (Cold)"]
    print("Testing Zipf (Should see 'Product A' mostly):")
    counts = {i:0 for i in items}
    for _ in range(100):
        choice = StatisticalGenerator.pick_zipfian_item(items)
        counts[choice] += 1
    print(counts)
    
    print("\nTesting Pareto Delays (Seconds):")
    for _ in range(10):
        print(f"{StatisticalGenerator.generate_pareto_delay(1, 5):.2f}s")