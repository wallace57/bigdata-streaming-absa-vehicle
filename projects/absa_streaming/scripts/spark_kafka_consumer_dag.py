#!/usr/bin/env python3
# ==========================================================
# consumer_postgres_streaming.py
# Kafka -> Spark Structured Streaming -> PhoBERT ABSA -> PostgreSQL
# - Tự động reload model nếu file weights thay đổi
# - Ghi cột model_version để trace
# ==========================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import from_json, col
import pandas as pd, torch, torch.nn as nn, re, json, sys, os, time
from transformers import AutoTokenizer, AutoModel
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# =================== Spark Session ===================
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

# =================== Kafka Source ===================
df_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "absa-reviews")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 50)
    .load()
)

df_json = df_stream.selectExpr("CAST(value AS STRING) AS json")

review_schema = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("review", T.StringType())
])

df_text = df_json.select(from_json(col("json"), review_schema).alias("d")).select("d.*")

# =================== Model / Config ===================
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
SAVE_DIR = "/opt/airflow/models/saved_absa_model"   # mounted volume
WEIGHTS_PATH = f"{SAVE_DIR}/pytorch_model.bin"
TOKENIZER_DIR = SAVE_DIR
CFG_PATH = f"{SAVE_DIR}/absa_config.json"

# Load config
if not os.path.exists(CFG_PATH):
    raise FileNotFoundError(f"Config not found: {CFG_PATH}")
with open(CFG_PATH, "r", encoding="utf-8") as f:
    cfg = json.load(f)

ASPECT_COLS = cfg["aspect_cols"]
NUM_CLASSES = cfg["num_classes"]
MAX_LEN = cfg.get("max_len", 128)
MODEL_NAME = cfg["model_name"]

# ============== Model class (same as training) ==============
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

# ============== Load initial model & tokenizer ==============
print("[Consumer] Initializing model and tokenizer...")
tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_DIR)
model = MultiTaskPhoBERT(MODEL_NAME, len(ASPECT_COLS), NUM_CLASSES)

if not os.path.exists(WEIGHTS_PATH):
    raise FileNotFoundError(f"Model weights not found: {WEIGHTS_PATH}")
model.load_state_dict(torch.load(WEIGHTS_PATH, map_location=DEVICE))
model.to(DEVICE)
model.eval()
last_model_mtime = os.path.getmtime(WEIGHTS_PATH)
model_version = time.strftime("%Y%m%d-%H%M%S", time.localtime(last_model_mtime))
print(f"[Consumer] Loaded model version: {model_version}")

# ============== Text normalization ==============
_re_nonvn = re.compile(r"[^\w\sáàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ]", re.UNICODE)
def normalize_text(text: str) -> str:
    text = str(text).lower().strip()
    text = _re_nonvn.sub(" ", text)
    return re.sub(r"\s+", " ", text)

# ============== Auto-reload logic ==============
def reload_model_if_updated():
    global model, tokenizer, last_model_mtime, model_version
    try:
        current_mtime = os.path.getmtime(WEIGHTS_PATH)
    except FileNotFoundError:
        return
    if current_mtime != last_model_mtime:
        print("[Consumer] 🔄 Detected updated model weights — reloading...")
        model.load_state_dict(torch.load(WEIGHTS_PATH, map_location=DEVICE))
        model.to(DEVICE)
        model.eval()
        last_model_mtime = current_mtime
        model_version = time.strftime("%Y%m%d-%H%M%S", time.localtime(last_model_mtime))
        print(f"[Consumer] ✅ Reloaded model. New model_version={model_version}")

# ============== Inference ==============
def infer_aspects(texts):
    reload_model_if_updated()
    texts = [normalize_text(t) for t in texts]
    enc = tokenizer(texts, padding=True, truncation=True, max_length=MAX_LEN, return_tensors="pt")
    with torch.no_grad():
        logits = model(enc["input_ids"].to(DEVICE), enc["attention_mask"].to(DEVICE))
        preds = torch.argmax(logits, dim=-1).cpu().numpy()
    return preds  # (batch_size, num_aspects)

# ============== foreachBatch ==============
def write_to_postgres(batch_df, batch_id):
    pdf = batch_df.toPandas()
    sys.stdout.reconfigure(encoding='utf-8')

    if pdf.empty:
        print(f"[Batch {batch_id}] ⚠️ No data.")
        return

    reviews = pdf["review"].fillna("").tolist()
    preds = infer_aspects(reviews)

    rows = []
    for txt, p in zip(reviews, preds):
        row = {"review_text": txt, "model_version": model_version}
        for a, l in zip(ASPECT_COLS, p):
            row[f"{a}_pred"] = int(l)
        rows.append(row)

    pdf_out = pd.DataFrame(rows)
    print(f"\n[Batch {batch_id}] Received {len(pdf_out)} rows:")
    print(pdf_out.head(5).to_string(index=False))

    # For testing: simulated crash every 100th batch (comment out in prod)
    if batch_id % 100 == 0 and batch_id != 0:
        print(f"[Batch {batch_id}] 💥 Simulated crash for test.")
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
        print(f"[Batch {batch_id}] ✅ Wrote {len(pdf_out)} rows to PostgreSQL.")
    except Exception as e:
        print(f"[Batch {batch_id}] ⚠️ Failed to write to Postgres: {str(e)}")
        print("Dumping sample to console:")
        print(pdf_out.head(3).to_json(orient="records", force_ascii=False, indent=2))

# ============== Start streaming ==============
query = (
    df_text.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

print("🚀 PhoBERT Streaming job started — listening to Kafka topic 'absa-reviews' ...")
query.awaitTermination()
