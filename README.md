XÂY DỰNG DATA WAREHOUSE TÍCH HỢP ĐA NGUỒN VÀ ỨNG DỤNG MACHINE LEARNING TRONG PHÂN TÍCH KHOÁ HỌC TRỰC TUYẾN
Dự án ứng dụng Machine Learning (XGBoost) để dự báo xu hướng khóa học và tự động hóa quy trình Marketing qua Email cho nền tảng khoá học (UE-UD).

## 🌟 Tính năng nổi bật
- **Cloud Data Warehouse:** Kết nối trực tiếp và xử lý dữ liệu từ Snowflake.
- **AI Core:** Sử dụng thuật toán XGBoost để xếp hạng xu hướng dựa trên hành vi người dùng (Views, Enrolls, Recency).
- **CI/CD Automation:** Tự động hóa hoàn toàn bằng GitHub Actions (Chạy định kỳ vào 8:00 AM Thứ Hai).
- **Serverless Architecture:** Vận hành 100% trên Cloud, không phụ thuộc máy tính cá nhân.

## 🛠 Tech Stack
- **Language:** Python 3.12
- **Database:** Snowflake (SQL)
- **ML Library:** XGBoost, Scikit-learn, Joblib
- **Automation:** GitHub Actions
- **Infrastructure:** SQLAlchemy, SMTP SSL

## 📊 Quy trình hoạt động
1. **ETL:** Trích xuất dữ liệu từ Snowflake.
2. **ML Inference:** Load mô hình `.pkl` và dự báo `Trend_Score`.
3. **Automated Mail:** Gửi danh sách Top 10 khóa học tiềm năng đến khách hàng.
