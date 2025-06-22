import constants
import node
import os
import time
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline
from generic import Generic

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 16))
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", 20.0))

class OverviewProcessor(Generic):
    def __init__(self):
        self.pipeline = self.init_model()
        self.batch = {}
        self.last_time = {}
        super().__init__()

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

    def process_message_batch(self, batch, pipeline):
        texts = []
        metadata = []

        for idx, (method, body) in enumerate(batch):
            try:
                body_split = body.split(constants.SEPARATOR)
                movie_id, budget, revenue, overview, title, client, message_id = body_split

                tokenized_text = pipeline.tokenizer(overview, padding=False, truncation=False, return_tensors="pt")
                if len(tokenized_text['input_ids'][0]) <= pipeline.tokenizer.model_max_length:
                    texts.append(overview)
                    metadata.append((movie_id, budget, revenue, title, client, message_id))
                    print(f"Processing movie {movie_id}: {title}")
                else:
                    print(f"Skipping movie {movie_id}: Overview too long ({len(tokenized_text['input_ids'][0])} tokens)")
            except Exception as e:
                print(f"Error processing message: {e}")
                continue

            if idx % 3 == 0:
                self.node_instance.consumer.connection.process_data_events(time_limit=0.1)
                

        if not texts:
            return

        results = pipeline(texts)

        for result, (movie_id, budget, revenue, title, client, message_id) in zip(results, metadata):
            sentiment_label = result['label']
            sentiment_score = str(result['score'])

            row_str = f"{movie_id}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{sentiment_label}{constants.SEPARATOR}{sentiment_score}{constants.SEPARATOR}{title}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
            self.node_instance.send_message(
                routing_key=str(movie_id[-1]),
                message=row_str
            )

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()

            if client not in self.clients_ended:
                self.clients_ended[client] = 0
            self.clients_ended[client] += 1
            print(f"Received END message from {client}, count: {self.clients_ended[client]}")


            if self.clients_ended[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                if self.batch[client]:
                    self.process_message_batch(self.batch[client], self.pipeline)
                    for m, _ in self.batch[client]:
                        print(f" [*] ACK for client {client}")
                        ch.basic_ack(delivery_tag=m.delivery_tag)

                self.node_instance.send_end_message_to_all_binds(client)
                self.batch.pop(client, None)
                self.last_time.pop(client, None)
                self.clients_ended.pop(client, None)
            
            ch.basic_ack(delivery_tag=method.delivery_tag)                

                
        else:

            body_split = body.decode().split(constants.SEPARATOR)
            budget = body_split[2]
            revenue = body_split[6]
            
            if float(budget) != 0 and float(revenue) != 0:
                movie_id = body_split[0]
                overview = body_split[3]
                title = body_split[7]
                client = body_split[8]
                message_id = body_split[9]
                if self.node_instance.is_repeated(message_id):
                    print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return 


                body = f"{movie_id}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{overview}{constants.SEPARATOR}{title}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                if client not in self.batch:
                    self.batch[client] = []
                if client not in self.last_time:
                    self.last_time[client] = time.time()
                
                self.batch[client].append((method, body))

                if len(self.batch[client]) >= BATCH_SIZE or (time.time() - self.last_time[client] >= BATCH_TIMEOUT):
                    self.process_message_batch(self.batch[client], self.pipeline)

                    for m, _ in self.batch[client]:
                        print(f" [*] ACK for client {client}")
                        ch.basic_ack(delivery_tag=m.delivery_tag)
                    self.batch[client] = []
                    self.last_time[client] = time.time()
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
        

if __name__ == '__main__':
    OverviewProcessor()