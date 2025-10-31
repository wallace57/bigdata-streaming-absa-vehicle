# ==========================================================
# consumer_postgres_streaming.py
# SE363 – Phát triển ứng dụng trên nền tảng dữ liệu lớn
# HopDT – Khoa Công nghệ Phần mềm, UIT
# ==========================================================
# Consumer đọc dữ liệu từ Kafka topic "absa-reviews"
# → chạy inference mô hình PhoBERT ABSA (saved_absa_model)
# → ghi kết quả vào PostgreSQL
# → Airflow sẽ giám sát và khởi động lại khi job bị dừng.
# ==========================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import from_json, col
import pandas as pd, torch, torch.nn as nn, re, json, sys
from transformers import AutoTokenizer, AutoModel
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# =================== 1️⃣ Spark Session ===================
scala_version = "2.12"
spark_version = "3.5.1"

spark = (
    SparkSession.builder
    .appName("Kafka_ABSA_Postgres_PhoBERT")
    .config("spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version},"
            "org.postgresql:postgresql:42.6.0,"
            "org.apache.kafka:kafka-clients:3.5.1")
    .config("spark.executor.instances", "1")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.streaming.checkpointLocation", "/opt/airflow/checkpoints/absa_streaming_checkpoint")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =================== 2️⃣ Kafka Source ===================
df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "absa-reviews")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 5)
    .load()
)

df_json = df_stream.selectExpr("CAST(value AS STRING) AS json")

review_schema = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("review", T.StringType())
])

df_text = df_json.select(from_json(col("json"), review_schema).alias("d")).select("d.*")

# =================== 3️⃣ PhoBERT Model ===================
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
SAVE_DIR = "/opt/airflow/models/saved_absa_model"

# Load config
with open(f"{SAVE_DIR}/absa_config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

ASPECT_COLS = cfg["aspect_cols"]
NUM_CLASSES = cfg["num_classes"]

# Model class
class MultiTaskPhoBERT(nn.Module):
    def __init__(self, model_name, num_aspects, num_classes):
        super().__init__()
        self.encoder = AutoModel.from_pretrained(model_name)
        hidden_size = self.encoder.config.hidden_size
        self.dropout = nn.Dropout(0.1)
        self.classifiers = nn.ModuleList([nn.Linear(hidden_size, num_classes) for _ in range(num_aspects)])
    def forward(self, input_ids, attention_mask):
        outputs = self.encoder(input_ids=input_ids, attention_mask=attention_mask, return_dict=True)
        cls_hidden = outputs.last_hidden_state[:, 0, :]
        pooled = self.dropout(cls_hidden)
        logits = [head(pooled) for head in self.classifiers]
        return torch.stack(logits, dim=1)  # (B, num_aspects, num_classes)

# Load model & tokenizer once
model = MultiTaskPhoBERT(cfg["model_name"], len(ASPECT_COLS), NUM_CLASSES)
model.load_state_dict(torch.load(f"{SAVE_DIR}/pytorch_model.bin", map_location=DEVICE))
model.to(DEVICE)
model.eval()

tokenizer = AutoTokenizer.from_pretrained(SAVE_DIR)

# =================== 4️⃣ Text Normalization ===================
_re_nonvn = re.compile(r"[^\w\sáàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ]", re.UNICODE)
def normalize_text(text: str) -> str:
    text = str(text).lower().strip()
    text = _re_nonvn.sub(" ", text)
    return re.sub(r"\s+", " ", text)

# =================== 5️⃣ Inference Function ===================
def infer_aspects(texts):
    texts = [normalize_text(t) for t in texts]
    enc = tokenizer(texts, padding=True, truncation=True, max_length=cfg["max_len"], return_tensors="pt")
    with torch.no_grad():
        logits = model(enc["input_ids"].to(DEVICE), enc["attention_mask"].to(DEVICE))
        preds = torch.argmax(logits, dim=-1).cpu().numpy()
    return preds  # (batch_size, num_aspects)

# =================== 6️⃣ foreachBatch ===================
def write_to_postgres(batch_df, batch_id):
    pdf = batch_df.toPandas()
    sys.stdout.reconfigure(encoding='utf-8')

    if pdf.empty:
        print(f"[Batch {batch_id}] ⚠️ Không có dữ liệu mới.")
        return

    reviews = pdf["review"].fillna("").tolist()
    preds = infer_aspects(reviews)

    rows = []
    for txt, p in zip(reviews, preds):
        row = {"ReviewText": txt}
        for a, l in zip(ASPECT_COLS, p):
            row[f"{a}_pred"] = int(l)
        rows.append(row)

    pdf_out = pd.DataFrame(rows)
    print(f"\n[Batch {batch_id}] Nhận {len(pdf_out)} dòng:")
    print(pdf_out.head(5).to_string(index=False))

    # Simulate crash every 5th batch to test Airflow restart
    if batch_id % 5 == 0:
        print(f"[Batch {batch_id}] 💥 Giả lập sự cố (crash mô phỏng).")
        raise Exception(f"Simulated crash at batch {batch_id}")

    try:
        (spark.createDataFrame(pdf_out)
            .write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/airflow")
            .option("dbtable", "absa_results")
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        print(f"[Batch {batch_id}] ✅ Ghi PostgreSQL thành công ({len(pdf_out)} dòng).")
    except Exception as e:
        print(f"[Batch {batch_id}] ⚠️ Ghi PostgreSQL thất bại: {str(e)}")
        print("Dữ liệu sẽ được log ra console:")
        print(pdf_out.head(3).to_json(orient="records", force_ascii=False, indent=2))

# =================== 7️⃣ Start Streaming ===================
query = (
    df_text.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print("🚀 PhoBERT Streaming job started — lắng nghe dữ liệu từ Kafka...")
query.awaitTermination()
