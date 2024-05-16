from pyspark import SparkConf
import logging 
import os 
import configparser
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import datediff, current_date, col, collect_list
from pyspark.sql import SparkSession
import smtplib
from email.mime.text import MIMEText
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText


# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Get the path to the parent parent directory
config_dir = os.path.abspath(os.path.join(os.getcwd()))

# Specify the path to config file
config_file_path = os.path.join(config_dir, "config.ini")
config = configparser.ConfigParser()
config.read(config_file_path)

config_file_path_json = os.path.join(config_dir, "config.json")

with open(config_file_path_json) as f:
    config_json = json.load(f)

def send_email(row):
    # Email configuration
    smtp_server = 'smtp.gmail.com'
    port = 587
    sender_email = "sony.sth8@gmail.com"
    receiver_email = row["email_id"] 
    app_password = 'oofzndilqxjbvjko'  # Use the app password generated for Mail/Other
    subject = 'SpicyBytes: Product Expiry Notification'

    image_path1 = os.path.join(config_dir, "images/spicy_img1.jpg") 

    # Create SMTP connection
    server = smtplib.SMTP(smtp_server, port)

    item_names = row["product_name"]
    expiry_dates = row["expected_expiry_date"]
    customer_name = row['customer_name']

    server.starttls()
    server.login(sender_email, app_password)

    # Create a multipart message container
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = row["email_id"]
    msg['To'] = receiver_email

    # Create HTML part
    html_body = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Grocery Item Expiry Notification</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 0;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }
            th, td {
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }
            th {
                background-color: #f2f2f2;
            }
            .container {
                max-width: 500px;
                margin: 20px auto;
                padding: 20px;
                background-color: #fff;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #333;
            }
            p {
                color: #666;
            }
            .expiry-date {
                color: #FF6347; /* Tomato color */
            }
        </style>
        </head>
        <body>
        
            <div class="container">
                <center><img src='cid:image1' alt="SpicyBytes Logo" width="200" height="50"></center>
                <h1>Grocery Item Expiry Notification</h1>
                <p>Dear """+customer_name+""",</p>
                <p>We would like to inform you that the following grocery items in your inventory are about to expire:</p>
                
                <table>
                    <tr>
                        <th>Item Name</th>
                        <th>Expiry Date</th>
                    </tr>
    """

    # Add rows to the table for each item
    for item_name, expiry_date in zip(item_names, expiry_dates):
        html_body += f"""
                <tr>
                    <td>{item_name}</td>
                    <td><span class="expiry-date">{expiry_date}</span></td>
                </tr>
        """

    # Add closing tags for HTML body
    html_body += """
            </table>
            
            <p>Please take necessary action to utilize or dispose of these items before they expire. If you do not intend to use them, you can sell them using our platform.</p>
            <p>Thank you for using our app!</p>
            <p>Sincerely,<br>Your Grocery Management App Team</p>
        </div>
    </body>
    </html>
    """

    html_part = MIMEText(html_body, 'html')
    msg.attach(html_part)

    # Attach image
    
    if os.path.exists(image_path1):
        with open(image_path1, 'rb') as img_file:
            image = MIMEImage(img_file.read())
            image.add_header('Content-ID', '<image1>')
            msg.attach(image)
            # image.add_header('Content-Disposition', 'attachment', filename=os.path.basename(image_path1))
            # msg.attach(image)

    # Send email
    server.sendmail(sender_email, receiver_email, msg.as_string())

    logger.info('-----------------------------------------------------')
    logger.info("Email sent successfully")

    # Close SMTP connection
    server.quit()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Read Parquet File") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .getOrCreate()   

    gcs_config = config["GCS"]["credentials_path"]
    raw_bucket_name = config["GCS"]["raw_bucket_name"]
    formatted_bucket_name = config["GCS"]["formatted_bucket_name"]

    days_considered = config_json["expiry_notification_days"]

    fuzzy_score_calc_method = config_json["product_matching"]["fuzzy_matching"]["score_calc_method"]
    fuzzy_threshold = config_json["product_matching"]["fuzzy_matching"]["threshold"]

    # spark = SparkSession.builder \
    #     .appName("Read Parquet File") \
    #     .config("spark.sql.repl.eagerEval.enabled", True) \
    #     .getOrCreate()
    spark = SparkSession.builder \
    .appName("GCS Files Read") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("google.cloud.auth.service.account.json.keyfile", gcs_config) \
    .getOrCreate()

    # Specify the path to the Parquet file
    # estimated_avg_expiry = "./data/formatted_zone/purchases_nearing_expiry"
    estimated_avg_expiry = spark.read.parquet('gs://'+formatted_bucket_name+'/purchases_nearing_expiry.*')

    # Read the Parquet file into a DataFrame
    estimated_avg_expiry_df = spark.read.parquet(estimated_avg_expiry)

    estimated_avg_expiry_df =estimated_avg_expiry_df.select("email_id", "customer_name", "product_name","purchase_date","avg_expiry_days","expected_expiry_date")
    estimated_avg_expiry_df = estimated_avg_expiry_df.withColumn("remaining_days", datediff(col("expected_expiry_date"), current_date()))

    estimated_avg_expiry_df = estimated_avg_expiry_df.filter((col("remaining_days") > 0) & (col("remaining_days") < days_considered))

    mail_group_df = estimated_avg_expiry_df.groupBy("email_id","customer_name").agg(
        collect_list("product_name").alias("product_name"),
        collect_list("expected_expiry_date").alias("expected_expiry_date")
    )

    mail_group_df.foreach(send_email) 
    