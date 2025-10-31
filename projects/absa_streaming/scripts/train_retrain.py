#!/usr/bin/env python3
# ============================================================
# train_retrain.py — ABSA Model Retraining Script for Airflow
# ============================================================
# Usage:
#   python3 train_retrain.py --output candidate_absa.pt --eval-log eval_result.json
#
# Description:
#   - Loads historical ABSA data
#   - Retrains model (mock or real PyABSA)
#   - Evaluates new model
#   - Writes metrics for Airflow DAG to compare
# ============================================================

import argparse
import os
import json
import random
import time
from datetime import datetime

# Optional: real PyABSA import (comment out if not installed)
# from pyabsa import ABSADatasetList, ABSAInstructionalTrainer, SentimentAnalyzer


def mock_train(output_path, eval_log):
    """
    Simulate retraining for demonstration or testing Airflow DAG
    """
    print("🧠 [train_retrain.py] Starting mock retraining...")
    time.sleep(5)

    # Simulate metrics
    best_acc = 0.83  # This should be loaded from metadata or DB
    new_acc = round(random.uniform(0.80, 0.90), 3)
    new_f1 = round(random.uniform(0.75, 0.88), 3)

    # Save "fake" model
    with open(output_path, "w") as f:
        f.write(f"FAKE_MODEL_{datetime.now().isoformat()}_ACC_{new_acc}")

    # Write evaluation result
    result = {
        "timestamp": datetime.now().isoformat(),
        "new_model_acc": new_acc,
        "new_model_f1": new_f1,
        "best_model_acc": best_acc,
    }
    with open(eval_log, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ [train_retrain.py] Done retraining. new_acc={new_acc}, best_acc={best_acc}")
    print(f"📦 Model saved at: {output_path}")
    print(f"📊 Eval log saved at: {eval_log}")


def real_train(output_path, eval_log):
    """
    Example for REAL retraining using PyABSA (if installed)
    """
    from pyabsa import ABSADatasetList, ABSAInstructionalTrainer, SentimentAnalyzer

    print("🚀 Starting real ABSA retraining with PyABSA...")
    dataset = ABSADatasetList.SemEval
    config = {
        "model_name": "bert-base-multilingual-cased",
        "num_epoch": 3,
        "learning_rate": 2e-5,
        "max_seq_len": 128,
        "device": "cuda" if torch.cuda.is_available() else "cpu",
    }

    # Train model
    trainer = ABSAInstructionalTrainer(
        dataset=dataset,
        config=config,
        save_path=os.path.dirname(output_path)
    )
    trainer.train()
    acc, f1 = trainer.evaluate()

    result = {
        "timestamp": datetime.now().isoformat(),
        "new_model_acc": acc,
        "new_model_f1": f1,
        "best_model_acc": 0.83,
    }

    trainer.save(output_path)
    with open(eval_log, "w") as f:
        json.dump(result, f, indent=2)

    print("✅ Training done with PyABSA.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="Path to save candidate model (e.g., candidate_absa.pt)")
    parser.add_argument("--eval-log", required=True, help="Path to save evaluation JSON result")
    parser.add_argument("--mode", choices=["mock", "real"], default="mock", help="mock or real training")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    os.makedirs(os.path.dirname(args.eval_log), exist_ok=True)

    if args.mode == "real":
        real_train(args.output, args.eval_log)
    else:
        mock_train(args.output, args.eval_log)

# import os
# import random
# import json
# import torch
# import torch.nn as nn
# import numpy as np
# import pandas as pd
# from tqdm.auto import tqdm
# from typing import List
# from transformers import AutoTokenizer, AutoModel
# from sklearn.metrics import f1_score, accuracy_score, classification_report
# from torch.utils.data import Dataset, DataLoader

# # === Config ===
# class Config:
#     base_model_dir = "models/saved_absa_model"   # existing trained model
#     train_csv = "/content/train_data_new.csv"    # new training data
#     val_csv   = "/content/val_data_new.csv"
#     test_csv  = "/content/test_data_new.csv"
#     epochs = 3
#     lr = 1e-5
#     batch_size = 16
#     weight_decay = 0.01
#     device = "cuda" if torch.cuda.is_available() else "cpu"
#     seed = 42

# cfg = Config()

# # === Seed ===
# def set_seed(seed=42):
#     random.seed(seed)
#     np.random.seed(seed)
#     torch.manual_seed(seed)
#     if torch.cuda.is_available():
#         torch.cuda.manual_seed_all(seed)
# set_seed(cfg.seed)

# # === Label mapping ===
# LABEL_MAP = {-1:0, 0:1, 1:2, 2:3}
# INV_LABEL_MAP = {v:k for k,v in LABEL_MAP.items()}

# def map_labels_row(row, aspects: List[str]):
#     return [LABEL_MAP[int(row[a])] for a in aspects]

# # === Dataset ===
# class ABSADataset(Dataset):
#     def __init__(self, df: pd.DataFrame, tokenizer, max_len: int, aspects: List[str]):
#         self.texts = df["Review"].astype(str).tolist()
#         self.labels = [map_labels_row(row, aspects) for _, row in df.iterrows()]
#         self.tokenizer = tokenizer
#         self.max_len = max_len

#     def __len__(self):
#         return len(self.texts)

#     def __getitem__(self, idx):
#         text = self.texts[idx]
#         encoding = self.tokenizer(
#             text,
#             truncation=True,
#             max_length=self.max_len,
#             padding="max_length",
#             return_tensors="pt"
#         )
#         return {
#             "input_ids": encoding["input_ids"].squeeze(0),
#             "attention_mask": encoding["attention_mask"].squeeze(0),
#             "labels": torch.tensor(self.labels[idx], dtype=torch.long)
#         }

# def get_dataloader(df, tokenizer, batch_size, shuffle, aspects, max_len):
#     ds = ABSADataset(df, tokenizer, max_len, aspects)
#     return DataLoader(ds, batch_size=batch_size, shuffle=shuffle, num_workers=2)

# # === Model ===
# class MultiTaskPhoBERT(nn.Module):
#     def __init__(self, model_name: str, num_aspects: int, num_classes: int, dropout_prob=0.1):
#         super().__init__()
#         self.encoder = AutoModel.from_pretrained(model_name)
#         hidden_size = self.encoder.config.hidden_size
#         self.dropout = nn.Dropout(dropout_prob)
#         self.classifiers = nn.ModuleList([nn.Linear(hidden_size, num_classes) for _ in range(num_aspects)])

#     def forward(self, input_ids, attention_mask):
#         outputs = self.encoder(input_ids=input_ids, attention_mask=attention_mask, return_dict=True)
#         cls_hidden = outputs.last_hidden_state[:, 0, :]
#         pooled = self.dropout(cls_hidden)
#         logits = torch.stack([head(pooled) for head in self.classifiers], dim=1)
#         return logits

# # === Utility ===
# def compute_class_weights(df: pd.DataFrame, aspects: List[str], num_classes: int, device):
#     weights = []
#     for a in aspects:
#         vals = df[a].astype(int).values
#         mapped = np.array([LABEL_MAP[int(v)] for v in vals])
#         counts = np.bincount(mapped, minlength=num_classes).astype(np.float32)
#         counts[counts == 0] = 1.0
#         total = counts.sum()
#         cw = total / (num_classes * counts)
#         cw = cw / cw.sum() * num_classes
#         weights.append(torch.tensor(cw, dtype=torch.float32).to(device))
#     return weights

# # === Training + Eval ===
# def train_one_epoch(model, loader, optimizer, criterion_list, device, num_aspects):
#     model.train()
#     running_loss = 0.0
#     for batch in tqdm(loader, desc="Train"):
#         input_ids = batch["input_ids"].to(device)
#         attention_mask = batch["attention_mask"].to(device)
#         labels = batch["labels"].to(device)
#         optimizer.zero_grad()
#         logits = model(input_ids, attention_mask)
#         loss = 0.0
#         for ai in range(num_aspects):
#             loss += criterion_list[ai](logits[:, ai, :], labels[:, ai])
#         loss = loss / num_aspects
#         loss.backward()
#         optimizer.step()
#         running_loss += loss.item() * input_ids.size(0)
#     return running_loss / len(loader.dataset)

# def evaluate(model, loader, device, aspects):
#     model.eval()
#     all_preds, all_labels = [], []
#     with torch.no_grad():
#         for batch in tqdm(loader, desc="Eval"):
#             input_ids = batch["input_ids"].to(device)
#             attention_mask = batch["attention_mask"].to(device)
#             labels = batch["labels"].to(device)
#             logits = model(input_ids, attention_mask)
#             preds = torch.argmax(logits, dim=-1)
#             all_preds.append(preds.cpu().numpy())
#             all_labels.append(labels.cpu().numpy())
#     all_preds = np.concatenate(all_preds, axis=0)
#     all_labels = np.concatenate(all_labels, axis=0)

#     metrics, macro_f1s = {}, []
#     for i, a in enumerate(aspects):
#         y_true, y_pred = all_labels[:, i], all_preds[:, i]
#         acc = accuracy_score(y_true, y_pred)
#         f1 = f1_score(y_true, y_pred, average="macro", zero_division=0)
#         metrics[a] = {"accuracy": acc, "f1_macro": f1}
#         macro_f1s.append(f1)
#     metrics["mean_macro_f1"] = float(np.mean(macro_f1s))
#     return metrics

# # === Main ===
# def main():
#     # Load config from saved model
#     with open(os.path.join(cfg.base_model_dir, "absa_config.json"), "r", encoding="utf-8") as f:
#         model_cfg = json.load(f)

#     model_name = model_cfg["model_name"]
#     aspects = model_cfg["aspect_cols"]
#     num_classes = model_cfg["num_classes"]
#     max_len = model_cfg["max_len"]

#     tokenizer = AutoTokenizer.from_pretrained(cfg.base_model_dir, use_fast=False)

#     # Load data
#     df_train = pd.read_csv(cfg.train_csv)
#     df_val   = pd.read_csv(cfg.val_csv)
#     df_test  = pd.read_csv(cfg.test_csv)

#     train_loader = get_dataloader(df_train, tokenizer, cfg.batch_size, True, aspects, max_len)
#     val_loader   = get_dataloader(df_val, tokenizer, cfg.batch_size, False, aspects, max_len)
#     test_loader  = get_dataloader(df_test, tokenizer, cfg.batch_size, False, aspects, max_len)

#     # Load model
#     model = MultiTaskPhoBERT(model_name, num_aspects=len(aspects), num_classes=num_classes)
#     model.load_state_dict(torch.load(os.path.join(cfg.base_model_dir, "pytorch_model.bin"), map_location=cfg.device))
#     model.to(cfg.device)

#     # Optimizer + loss
#     weights = compute_class_weights(df_train, aspects, num_classes, cfg.device)
#     criterions = [nn.CrossEntropyLoss(weight=w) for w in weights]
#     optimizer = torch.optim.AdamW(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)

#     best_val_f1 = 0.0
#     for epoch in range(1, cfg.epochs + 1):
#         print(f"\n===== Epoch {epoch}/{cfg.epochs} =====")
#         loss = train_one_epoch(model, train_loader, optimizer, criterions, cfg.device, len(aspects))
#         print(f"Train loss: {loss:.4f}")

#         val_metrics = evaluate(model, val_loader, cfg.device, aspects)
#         print("Val mean_macro_f1:", val_metrics["mean_macro_f1"])
#         for a in aspects:
#             print(f"  {a}: acc={val_metrics[a]['accuracy']:.4f}, f1={val_metrics[a]['f1_macro']:.4f}")

#         if val_metrics["mean_macro_f1"] > best_val_f1:
#             best_val_f1 = val_metrics["mean_macro_f1"]
#             torch.save(model.state_dict(), os.path.join(cfg.base_model_dir, "pytorch_model.bin"))
#             print("✅ Updated model saved!")

#     # Save tokenizer + config again (in case aspects changed)
#     tokenizer.save_pretrained(cfg.base_model_dir)
#     with open(os.path.join(cfg.base_model_dir, "absa_config.json"), "w", encoding="utf-8") as f:
#         json.dump(model_cfg, f, ensure_ascii=False, indent=2)

#     print("✅ Retraining complete. Model refreshed in:", cfg.base_model_dir)

#     # Evaluate on test
#     test_metrics = evaluate(model, test_loader, cfg.device, aspects)
#     print("\n=== Test Metrics ===")
#     print("Mean macro F1:", test_metrics["mean_macro_f1"])
#     for a in aspects:
#         print(f"{a}: acc={test_metrics[a]['accuracy']:.4f}, f1_macro={test_metrics[a]['f1_macro']:.4f}")

# if __name__ == "__main__":
#     main()
