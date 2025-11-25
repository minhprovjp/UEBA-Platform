import mysql.connector
import pandas as pd

# CẤU HÌNH
DB_CONFIG = {
    "user": "root", 
    "password": "root",  # <--- Thay password của bạn
    "host": "localhost"
}

def check_health():
    print("🏥 ĐANG KIỂM TRA SỨC KHỎE HỆ THỐNG MYSQL...\n")
    
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 1. KIỂM TRA DATABASE
        cursor.execute("SHOW DATABASES")
        dbs = [d[0] for d in cursor.fetchall()]
        required_dbs = ['sales_db', 'hr_db']
        
        print(f"1️⃣  KIỂM TRA DATABASE:")
        for db in required_dbs:
            status = "✅ OK" if db in dbs else "❌ THIẾU"
            print(f"   - {db:<15} {status}")
            
        # 2. KIỂM TRA CHI TIẾT TỪNG DB
        for db in required_dbs:
            if db not in dbs: continue
            
            print(f"\n2️⃣  KIỂM TRA BẢNG & DỮ LIỆU TRONG '{db}':")
            cursor.execute(f"USE {db}")
            cursor.execute("SHOW TABLES")
            tables = [t[0] for t in cursor.fetchall()]
            
            if not tables:
                print("   ⚠️  Cảnh báo: Database rỗng, chưa có bảng nào!")
                continue
                
            # Tạo bảng báo cáo
            report_data = []
            for t in tables:
                # Đếm số dòng
                cursor.execute(f"SELECT COUNT(*) FROM {t}")
                count = cursor.fetchone()[0]
                
                # Lấy danh sách cột
                cursor.execute(f"DESCRIBE {t}")
                cols = [col[0] for col in cursor.fetchall()]
                col_str = ", ".join(cols)
                if len(col_str) > 50: col_str = col_str[:47] + "..."
                
                report_data.append({
                    "Table": t,
                    "Rows": count,
                    "Columns (Preview)": col_str
                })
            
            # In bảng đẹp
            df = pd.DataFrame(report_data)
            print(df.to_string(index=False))

        # 3. KIỂM TRA USER
        print(f"\n3️⃣  KIỂM TRA USER HỆ THỐNG:")
        cursor.execute("SELECT user, host FROM mysql.user WHERE user LIKE '%_user_%' OR user LIKE '%insider%'")
        users = cursor.fetchall()
        
        if users:
            print(f"   ✅ Tìm thấy {len(users)} users mô phỏng (VD: {users[0][0]}).")
        else:
            print("   ❌ KHÔNG TÌM THẤY USER MÔ PHỎNG NÀO! (Cần chạy lại setup_full_environment.py)")

        conn.close()
        print("\n---------------------------------------------------")
        print("KẾT LUẬN:")
        if len(users) > 0 and all(db in dbs for db in required_dbs):
            print("🎉 Môi trường đã SETUP TỐT. Lỗi là do Script chạy (Step 3) hoặc Query sai.")
        else:
            print("💀 Môi trường THIẾU SÓT. Hãy chạy lại file 'setup_full_environment.py' ngay.")

    except Exception as e:
        print(f"❌ LỖI KẾT NỐI: {e}")
        print("   -> Kiểm tra lại password root trong file này.")

if __name__ == "__main__":
    check_health()