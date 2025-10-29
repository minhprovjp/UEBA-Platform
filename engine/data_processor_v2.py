# engine/data_processor_v2.py

import pandas as pd
import numpy as np
import time
import json
import logging
import os
import sys
from datetime import time as dt_time

# --- Cài đặt các thư viện cần thiết ---
# pip install pandas scikit-learn matplotlib seaborn joblib tzlocal
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, ConfusionMatrixDisplay

import matplotlib.pyplot as plt
import seaborn as sns

# --- Thiết lập Path và Logging ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import *
from engine.utils import extract_query_features # Tận dụng lại hàm extract feature cũ

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [DataProcessorV2] - %(message)s')


# ==============================================================================
# BƯỚC 1: FEATURE ENGINEERING NÂNG CAO VÀ TỐI ƯU HÓA
# ==============================================================================
def feature_engineering_advanced(df: pd.DataFrame) -> pd.DataFrame:
    """Biến đổi toàn bộ DataFrame thành các feature số học một cách hiệu quả."""
    logging.info("Bắt đầu Advanced Feature Engineering...")
    df_featured = df.copy()

    # --- 1. Trích xuất feature từ Timestamp (Thao tác Vectorized, rất nhanh) ---
    df_featured['hour'] = df_featured['timestamp'].dt.hour
    df_featured['day_of_week'] = df_featured['timestamp'].dt.dayofweek
    df_featured['is_weekend'] = (df_featured['day_of_week'] >= 5).astype(int)
    
    # Tính "time_since_last_query" cho mỗi user
    # Sắp xếp theo user và timestamp, sau đó dùng `diff()` để tính khoảng cách thời gian
    df_featured = df_featured.sort_values(by=['user', 'timestamp'])
    df_featured['time_since_last_query'] = df_featured.groupby('user')['timestamp'].diff().dt.total_seconds().fillna(0)

    # --- 2. Trích xuất feature từ Query (Tối ưu hóa bằng cách gọi `apply` một lần) ---
    logging.info("Trích xuất các feature từ câu lệnh Query...")
    # `extract_query_features` trả về một dict, chúng ta sẽ chuyển nó thành các cột
    query_features_df = df_featured['query'].apply(extract_query_features).apply(pd.Series)
    
    # Nối các feature mới vào
    df_featured = pd.concat([df_featured, query_features_df], axis=1)
    
    # --- 3. Frequency Encoding cho các cột Categorical (Vectorized, rất nhanh) ---
    logging.info("Mã hóa các trường User, IP, Database...")
    for col in ['user', 'client_ip', 'database']:
        # Tính tần suất xuất hiện của mỗi giá trị
        freq_map = df_featured[col].value_counts(normalize=True)
        # Ánh xạ tần suất đó vào một cột mới
        df_featured[f'{col}_freq'] = df_featured[col].map(freq_map).fillna(0)

    logging.info("Hoàn thành Advanced Feature Engineering.")
    return df_featured

# ==============================================================================
# BƯỚC 2: THỬ NGHIỆM CÁC THUẬT TOÁN UNSUPERVISED
# ==============================================================================
def run_unsupervised_experiments(X: pd.DataFrame, feature_names: list):
    """Chạy và so sánh các mô hình Unsupervised."""
    logging.info("Bắt đầu thử nghiệm các mô hình Unsupervised...")
    results = {}
    
    # Chuẩn hóa dữ liệu một lần duy nhất
    X_scaled = StandardScaler().fit_transform(X)
    
    # Danh sách các mô hình để thử nghiệm
    models = {
        "IsolationForest": IsolationForest(contamination='auto', random_state=42, n_jobs=-1),
        "LocalOutlierFactor": LocalOutlierFactor(n_neighbors=20, contamination='auto', novelty=True, n_jobs=-1),
        "OneClassSVM": OneClassSVM(nu=0.05, kernel="rbf", gamma='auto')
    }
    
    for name, model in models.items():
        console_color = "cyan" if name == "IsolationForest" else "magenta" if name == "LocalOutlierFactor" else "green"
        logging.info(f"[{console_color}]Đang huấn luyện và dự đoán với {name}...[/{console_color}]")
        start_time = time.time()
        
        model.fit(X_scaled)
        
        if hasattr(model, "decision_function"):
            scores = model.decision_function(X_scaled)
        else: # LOF dùng score_samples
            scores = model.score_samples(X_scaled)
        
        predictions = model.predict(X_scaled)
        # Chuyển đổi kết quả: -1 (bất thường) -> 1, 1 (bình thường) -> 0
        anomaly_labels = (predictions == -1).astype(int)
        
        duration = time.time() - start_time
        results[name] = {
            "scores": scores,
            "predictions": anomaly_labels,
            "duration": duration,
            "anomaly_count": np.sum(anomaly_labels)
        }
        logging.info(f"[{console_color}]Hoàn thành {name} trong {duration:.2f} giây. Phát hiện {results[name]['anomaly_count']} bất thường.[/{console_color}]")
        
    return results

# ==============================================================================
# BƯỚC 3: TRỰC QUAN HÓA VÀ SO SÁNH
# ==============================================================================
def plot_results_comparison(unsupervised_results: dict):
    """Tạo và lưu các biểu đồ so sánh hiệu suất."""
    logging.info("Đang tạo biểu đồ so sánh...")
    
    # Dữ liệu cho biểu đồ
    names = list(unsupervised_results.keys())
    counts = [res['anomaly_count'] for res in unsupervised_results.values()]
    durations = [res['duration'] for res in unsupervised_results.values()]

    # Tạo 2 biểu đồ con trên cùng một hình
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle('So sánh Hiệu suất các Thuật toán Unsupervised', fontsize=16)

    # Biểu đồ 1: Số lượng bất thường được phát hiện
    sns.barplot(x=names, y=counts, ax=ax1, palette='viridis')
    ax1.set_title("Số lượng Bất thường được phát hiện")
    ax1.set_ylabel("Số lượng")
    ax1.tick_params(axis='x', rotation=45)
    for index, value in enumerate(counts):
        ax1.text(index, value, str(value), ha='center')

    # Biểu đồ 2: Thời gian xử lý
    sns.barplot(x=names, y=durations, ax=ax2, palette='plasma')
    ax2.set_title("Thời gian Xử lý (giây)")
    ax2.set_ylabel("Thời gian (s)")
    ax2.tick_params(axis='x', rotation=45)
    for index, value in enumerate(durations):
        ax2.text(index, value, f'{value:.2f}s', ha='center')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig("unsupervised_comparison.png")
    logging.info("Đã lưu biểu đồ so sánh vào 'unsupervised_comparison.png'")
    plt.close()

# ==============================================================================
# BƯỚC 4: TRIỂN KHAI LUỒNG HỌC BÁN GIÁM SÁT (SEMI-SUPERVISED)
# ==============================================================================
def run_semi_supervised_flow(df_featured: pd.DataFrame, X: pd.DataFrame, feature_names: list, labeled_data: pd.DataFrame):
    """Thực hiện luồng học bán giám sát (Self-Training)."""
    logging.info("[bold blue]Bắt đầu luồng học Bán giám sát (Semi-Supervised)...[/bold blue]")
    
    # --- 1. Huấn luyện mô hình Unsupervised ban đầu ---
    logging.info("Bước 1: Huấn luyện mô hình Unsupervised ban đầu (IsolationForest)...")
    X_scaled = StandardScaler().fit_transform(X)
    unsupervised_model = IsolationForest(contamination='auto', random_state=42, n_jobs=-1)
    unsupervised_model.fit(X_scaled)
    
    # --- 2. Tạo Pseudo-Labels ---
    logging.info("Bước 2: Tạo nhãn giả (Pseudo-Labels)...")
    df_featured['pseudo_label'] = (unsupervised_model.predict(X_scaled) == -1).astype(int)
    
    # --- 3. Chuẩn bị & Kết hợp Nhãn ---
    logging.info("Bước 3: Chuẩn bị và kết hợp nhãn...")
    # Tạo một key duy nhất để join, tránh lỗi do kiểu dữ liệu
    df_featured['join_key'] = df_featured['timestamp'].astype(str) + df_featured['user'].astype(str) + df_featured['query']
    labeled_data['timestamp'] = pd.to_datetime(labeled_data['timestamp'])
    labeled_data['join_key'] = labeled_data['timestamp'].astype(str) + labeled_data['user'].astype(str) + labeled_data['query']
    
    # Chỉ giữ lại các cột cần thiết từ dữ liệu có nhãn
    labeled_subset = labeled_data[['join_key', 'is_anomaly_label']]
    
    # Gộp 2 bộ dữ liệu
    df_combined = pd.merge(df_featured, labeled_subset, on='join_key', how='left')
    
    # Ưu tiên nhãn thật: nếu is_anomaly_label không rỗng, dùng nó, nếu không thì dùng nhãn giả
    final_labels = df_combined['is_anomaly_label'].fillna(df_combined['pseudo_label']).astype(int)
    
    # --- 4. Huấn luyện Mô hình Supervised cuối cùng ---
    logging.info("Bước 4: Huấn luyện mô hình Supervised (RandomForest) trên bộ dữ liệu kết hợp...")
    X_train, X_test, y_train, y_test = train_test_split(X, final_labels, test_size=0.3, random_state=42, stratify=final_labels)
    
    final_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', n_jobs=-1)
    final_model.fit(X_train, y_train)
    
    # --- 5. Đánh giá và Trực quan hóa ---
    logging.info("Bước 5: Đánh giá mô hình Semi-Supervised cuối cùng...")
    y_pred = final_model.predict(X_test)
    
    report = classification_report(y_test, y_pred)
    logging.info("Báo cáo Phân loại:\n" + report)
    
    # Vẽ ma trận nhầm lẫn
    plt.figure(figsize=(8, 6))
    ConfusionMatrixDisplay.from_estimator(final_model, X_test, y_test, cmap=plt.cm.Blues)
    plt.title("Ma trận nhầm lẫn - Mô hình Semi-Supervised")
    plt.tight_layout()
    plt.savefig("semi_supervised_confusion_matrix.png")
    logging.info("Đã lưu ma trận nhầm lẫn vào 'semi_supervised_confusion_matrix.png'")
    plt.close()
    
    return final_model

# ==============================================================================
# HÀM CHÍNH ĐIỀU PHỐI
# ==============================================================================
def main_processor_v2(input_df: pd.DataFrame):
    """
    Hàm chính điều phối toàn bộ quá trình xử lý, thử nghiệm và báo cáo.
    """
    if input_df is None or input_df.empty:
        logging.error("Không có dữ liệu đầu vào để xử lý.")
        return

    # --- 1. Feature Engineering nâng cao ---
    df_featured = feature_engineering_advanced(input_df)
    
    # --- 2. Chuẩn bị dữ liệu cho mô hình (chỉ các cột số) ---
    feature_cols = df_featured.select_dtypes(include=np.number).columns.tolist()
    X = df_featured[feature_cols]
    
    # --- 3. Chạy và so sánh các mô hình Unsupervised ---
    unsupervised_results = run_unsupervised_experiments(X, feature_cols)
    plot_results_comparison(unsupervised_results)
    
    # --- 4. Thực hiện luồng Semi-Supervised ---
    feedback_file = 'feedback.csv' # Lấy từ config
    if os.path.exists(feedback_file) and os.path.getsize(feedback_file) > 0:
        try:
            labeled_data = pd.read_csv(feedback_file)
            if not labeled_data.empty:
                final_model = run_semi_supervised_flow(df_featured.copy(), X.copy(), feature_cols, labeled_data)
                # (Có thể làm gì đó với `final_model`, ví dụ: lưu lại)
        except Exception as e:
            logging.error(f"Lỗi khi xử lý file feedback: {e}")
    else:
        logging.warning("Không tìm thấy file feedback.csv hoặc file rỗng. Bỏ qua luồng Semi-Supervised.")
        
    logging.info("[bold green]Toàn bộ quá trình thử nghiệm đã hoàn tất![/bold green]")
    # Hàm này có thể trả về mô hình tốt nhất hoặc một báo cáo tổng hợp
    return unsupervised_results


# --- Điểm khởi đầu để chạy file này độc lập ---
if __name__ == '__main__':
    # Giả lập việc đọc file từ engine_runner
    log_file_path = PARSED_MYSQL_LOG_FILE_PATH # Hoặc một file CSV lớn khác
    
    if not os.path.exists(log_file_path) or os.path.getsize(log_file_path) == 0:
        logging.error(f"File log '{log_file_path}' không tồn tại hoặc rỗng. Vui lòng tạo dữ liệu trước.")
    else:
        logging.info(f"Đang đọc dữ liệu từ: {log_file_path}")
        # Đọc file CSV
        df_logs = pd.read_csv(log_file_path)
        
        # Tiền xử lý timestamp
        df_logs['timestamp'] = pd.to_datetime(df_logs['timestamp'], errors='coerce')
        df_logs.dropna(subset=['timestamp'], inplace=True)

        # Chạy quy trình xử lý chính
        main_processor_v2(df_logs)