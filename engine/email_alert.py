# email_alert.py
# PRODUCTION 2025 — Beautiful HTML Alert with All New Features

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
import logging

def send_email_alert(subject, results_df, top_n=5):
    """
    Send rich HTML email with top anomalies including new features
    """
    if results_df.empty:
        return

    # Top anomalies
    df = results_df.nlargest(top_n, 'ml_anomaly_score') if 'ml_anomaly_score' in results_df.columns else results_df.head(top_n)

    html_body = """
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <h2 style="color: #d32f2f;">MySQL UBA — High-Risk Activity Detected</h2>
        <p><strong>Time:</strong> {now}</p>
        <p><strong>Total Alerts:</strong> {total}</p>
        <hr>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; width: 100%;">
            <tr style="background: #f44336; color: white;">
                <th>Score</th>
                <th>User</th>
                <th>Attack Type</th>
                <th>Query (Normalized)</th>
                <th>Tables Touched</th>
                <th>Rows</th>
                <th>Time</th>
            </tr>
    """.format(now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total=len(results_df))

    for _, row in df.iterrows():
        score = row.get('ml_anomaly_score', 0.0)
        score_color = "#d32f2f" if score > 0.9 else "#ff9800" if score > 0.7 else "#4caf50"
        
        html_body += f"""
            <tr>
                <td style="color: {score_color}; font-weight: bold;">{score:.3f}</td>
                <td><strong>{row['user']}</strong></td>
                <td>{row.get('attack_type', row.get('anomaly_type', 'unknown'))}</td>
                <td><code style="font-size: 11px;">{row.get('normalized_query', row['query'][:80])}...</code></td>
                <td>{row.get('accessed_sensitive_tables', 0)} sensitive</td>
                <td>{row.get('rows_returned', 0)}</td>
                <td>{row['timestamp'][:19].replace('T', ' ')}</td>
            </tr>
        """

    html_body += """
        </table>
        <hr>
        <p><small>MySQL UBA Engine 2025 — Real-time Behavioral Anomaly Detection</small></p>
    </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg['From'] = formataddr(("MySQL UBA Alert", "uba@yourcompany.com"))
    msg['To'] = ", ".join(["admin@company.com", "soc@company.com"])
    msg['Subject'] = subject

    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login("your_email@gmail.com", "your_app_password")
            server.send_message(msg)
        logging.info("Alert email sent successfully")
    except Exception as e:
        logging.error(f"Email failed: {e}")