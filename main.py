import pandas as pd
import numpy as np
import smtplib
import joblib
import os
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# --- BƯỚC 1: CẤU HÌNH BẢO MẬT (Lấy từ GitHub Secrets) ---
SNOW_USER = os.getenv('SNOWFLAKE_USER')
SNOW_PASS = os.getenv('SNOWFLAKE_PASSWORD')
SNOW_ACC = os.getenv('SNOWFLAKE_ACCOUNT')
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_PASS = os.getenv('GMAIL_APP_PASSWORD')

# --- BƯỚC 2: KẾT NỐI SNOWFLAKE ---
engine = create_engine(URL(
    user=SNOW_USER,
    password=SNOW_PASS,
    account=SNOW_ACC,
    database='PROJECT_CLOUD_DB',
    schema='PUBLIC',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
))

def send_bulk_trending_emails():
    try:
        print(f"🔄 [{time.ctime()}] Bắt đầu tiến trình gửi mail...")

        # --- CHẾ ĐỘ TEST: GỬI ĐẾN MAIL CHỈ ĐỊNH ---
        # Nếu muốn gửi cho khách hàng thật sau này, hãy comment dòng dưới lại
        user_list = ["nguyenthiyennhithienbinh@gmail.com"] 
        print(f"🧪 Đang chạy TEST gửi đến: {user_list}")

        # 1. Dự báo Top 10 (Xử lý dữ liệu AI)
        query_data = """
        SELECT C.COURSEID, C.TITLE, C.CATEGORY, C.VIEWS, C.PRICE, E.TIMEID
        FROM DIM_COURSE_CLOUD C
        LEFT JOIN FACT_ENROLLMENT_CLOUD E ON C.COURSEID = E.COURSEID
        """
        df_raw = pd.read_sql(query_data, engine)
        df_raw.columns = [col.upper() for col in df_raw.columns]

        enroll_counts = df_raw.groupby('COURSEID').size().reset_index(name='TOTAL_ENROLLMENTS')
        df_final = df_raw.drop_duplicates(subset=['COURSEID']).merge(enroll_counts, on='COURSEID')
        df_final['VIEWS'] = pd.to_numeric(df_final['VIEWS'], errors='coerce').fillna(0)
        df_final['CONVERSION_RATE'] = np.where(df_final['VIEWS'] > 0, df_final['TOTAL_ENROLLMENTS'] / df_final['VIEWS'], 0)

        # Xử lý Recency Score
        df_raw['TIMEID_NUM'] = pd.to_numeric(df_raw['TIMEID'].astype(str).str.extract(r'(\d+)', expand=False)).fillna(0)
        max_time = df_raw['TIMEID_NUM'].max()
        recency = df_raw.groupby('COURSEID')['TIMEID_NUM'].max().reset_index(name='MAX_TIMEID')
        df_final = df_final.merge(recency, on='COURSEID')
        df_final['RECENCY_SCORE'] = df_final['MAX_TIMEID'] / max_time if max_time != 0 else 0

        # 2. Load Model
        model = joblib.load('xgb_model_real.pkl')
        features = model.feature_names_in_
        df_final['TREND_SCORE'] = model.predict_proba(df_final[features].fillna(0))[:, 1]
        top_10 = df_final.sort_values(by='TREND_SCORE', ascending=False).head(10)

        # 3. Tạo nội dung Email
        table_rows = "".join([
            f"<tr><td style='padding:10px; border-bottom:1px solid #eee;'>{r['TITLE']}</td>"
            f"<td style='padding:10px; border-bottom:1px solid #eee;'>{r['CATEGORY']}</td>"
            f"<td style='padding:10px; border-bottom:1px solid #eee; color:#1a73e8; font-weight:bold;'>{r['TREND_SCORE']:.2f}</td></tr>"
            for _, r in top_10.iterrows()
        ])

        html_template = f"""
        <html>
          <body style="font-family: Arial, sans-serif; color: #333;">
            <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 20px; border-radius: 10px;">
              <h2 style="color: #1a73e8; text-align: center;">🚀 Top 10 Khóa học Xu hướng</h2>
              <table style="width:100%; border-collapse:collapse;">
                <tr style="background-color: #f8f9fa;">
                  <th style="padding:10px; text-align:left; border-bottom:2px solid #1a73e8;">Khóa học</th>
                  <th style="padding:10px; text-align:left; border-bottom:2px solid #1a73e8;">Ngành</th>
                  <th style="padding:10px; text-align:left; border-bottom:2px solid #1a73e8;">AI Score</th>
                </tr>
                {table_rows}
              </table>
              <p style="font-size: 11px; color: #888; text-align: center; margin-top: 20px;">Email tự động từ hệ thống Learnify.</p>
            </div>
          </body>
        </html>
        """

        # 4. Gửi Mail
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASS)
            for recipient in user_list:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = "🔥 [TEST] TOP 10 KHÓA HỌC MỚI NHẤT"
                msg["From"] = f"Learnify AI <{GMAIL_USER}>"
                msg["To"] = recipient
                msg.attach(MIMEText(html_template, "html"))
                server.sendmail(GMAIL_USER, recipient, msg.as_string())

        print(f"✅ Đã gửi thành công cho {len(user_list)} khách hàng.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

# --- KÍCH HOẠT ---
if __name__ == "__main__":
    send_bulk_trending_emails()
