import os
from transformers import AutoTokenizer, AutoModelForSequenceClassification

MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english"
MODEL_DIR = "/models/distilbert_model"

if os.path.exists(MODEL_DIR) and os.path.isfile(os.path.join(MODEL_DIR, "config.json")):
    print("Modelo ya descargado.")
else:
    print("Descargando modelo...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    os.makedirs(MODEL_DIR, exist_ok=True)
    tokenizer.save_pretrained(MODEL_DIR)
    model.save_pretrained(MODEL_DIR)
    print("Descarga completada en", MODEL_DIR)