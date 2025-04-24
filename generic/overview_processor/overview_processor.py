import constants
import node
import os
import time
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from generic import Generic

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 20))
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", 10.0))

class OverviewProcessor(Generic):
    def __init__(self):
        pipeline = self.init_model()
        message_counter = 0
        batch = []
        last_time = time.time()
        super().__init__()
        print(f"Total messages sent: {self.message_counter}")

    def init_model(self):
        """Inicializa el modelo de clasificación de texto y retorna el pipeline."""
        print("Loading model from disk...")
        model = AutoModelForSequenceClassification.from_pretrained('/models/distilbert_model')
        tokenizer = AutoTokenizer.from_pretrained("/models/distilbert_model")
        device = 0 if torch.cuda.is_available() else -1
        pipeline = TextClassificationPipeline(model=model, tokenizer=tokenizer, device=device)
        print(f"Model loaded")
        with open("/tmp/model_ready", "w") as f:
            f.write("ready")

        return pipeline

    def process_message_batch(self, batch, pipeline, message_counter):
        """Procesa un lote de mensajes y publica los resultados."""
        texts = []
        metadata = []

        for method, body in batch:
            try:
                body_split = body.decode().split(constants.SEPARATOR)
                movie_id, budget, revenue, overview, title = body_split

                tokenized_text = pipeline.tokenizer(overview, padding=False, truncation=False, return_tensors="pt")
                if len(tokenized_text['input_ids'][0]) <= pipeline.tokenizer.model_max_length:
                    texts.append(overview)
                    metadata.append((movie_id, budget, revenue, title))
                    print(f"Processing movie {movie_id}: {title}")
                else:
                    print(f"Skipping movie {movie_id}: Overview too long ({len(tokenized_text['input_ids'][0])} tokens)")
            except Exception as e:
                print(f"Error processing message: {e}")
                continue

        if not texts:
            return

        results = pipeline(texts)

        for result, (movie_id, budget, revenue, title) in zip(results, metadata):
            sentiment_label = result['label']
            sentiment_score = str(result['score'])

            row_str = f"{movie_id}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{sentiment_label}{constants.SEPARATOR}{sentiment_score}{constants.SEPARATOR}{title}"
            self.node_instance.send_message(
                routing_key=str(movie_id[-1]),
                message=row_str
            )
            message_counter += 1
        return message_counter
    


    def callback(self, ch, method, _properties, body):
        if body.decode() == constants.END:
            self.ended += 1
            print(f"Received END message from {method.routing_key}, count: {self.ended}")
            if self.ended == self.node_instance.total_binds():
                print("Received EOF for all movies")
                if self.batch:
                    self.message_counter = self.process_message_batch(self.batch, self.pipeline, self.message_counter)
                self.node_instance.send_end_message_to_all()
                self.node_instance.stop_consuming_and_close_connection(0)
                self.node_instance.close_publisher_connection()
                
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            if len(body_split) != 5:
                print("Skipping invalid message: Incorrect format")
                print(body.decode())
                return

            _, budget, revenue, _, _ = body_split

            if int(budget) == 0 or float(revenue) == 0.0:
                return
            self.batch.append((method, body))

            if len(self.batch) >= BATCH_SIZE or (time.time() - self.last_time >= BATCH_TIMEOUT):
                self.message_counter = self.process_message_batch(self.batch, self.pipeline, self.message_counter)
                self.batch = []
                self.last_time = time.time()
        

if __name__ == '__main__':
    OverviewProcessor()