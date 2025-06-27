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
                movie_id, budget, revenue, overview, title, client, message_id, _node_id = body_split

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

            row_str = f"{movie_id}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{sentiment_label}{constants.SEPARATOR}{sentiment_score}{constants.SEPARATOR}{title}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
            self.node_instance.send_message(
                routing_key=str(movie_id[-1]),
                message=row_str
            )

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.CLIENT_TIMEOUT):
            client = body.decode()[len(constants.CLIENT_TIMEOUT):].strip()
            #self.check_batch(client, last_eof=True)
            self.clean_batch(client, ch)
            print(f" [*] Received timeout for client {client}")

            if client not in self.clients_timeout:
                self.clients_timeout[client] = time.time()
                self.persist_timeout()

            if client in self.clients_ended:
                print(f" [*] Removing client {client} from EOF list due to timeout.")
                self.clients_ended.pop(client, None)
                self.persist_eof()


            if client in self.batch:
                print(f" [*] Removing client {client} from batch due to timeout.")
                self.batch.pop(client, None)

            for node_id in self.node_instance.last_message_id:
                if self.node_instance.last_message_id[node_id].pop(client, None) is not None:
                    self.persist_state()
                

            self.node_instance.send_timeout_message(
                routing_key=method.routing_key,
                client=client
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            if not self.should_process(client):
                print(f" [*] Ignoring EOF for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            if client not in self.clients_ended:
                self.clients_ended[client] = []

            if method.routing_key in self.clients_ended[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended[client].append(method.routing_key)

            if len(self.clients_ended[client]) == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                if self.batch[client]:
                    self.process_message_batch(self.batch[client], self.pipeline)
                    for m, _ in self.batch[client]:
                        ch.basic_ack(delivery_tag=m.delivery_tag)

                self.node_instance.send_end_message_to_all_binds(client)
                self.batch.pop(client, None)
                self.last_time.pop(client, None)
                self.clients_ended.pop(client, None)
            self.persist_eof()
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
                if not self.should_process(client):
                    print(f" [*] Ignoring message for client {client} due to timeout.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                
                message_id = body_split[9]

                body = f"{movie_id}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{revenue}{constants.SEPARATOR}{overview}{constants.SEPARATOR}{title}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
                if client not in self.batch:
                    self.batch[client] = []
                if client not in self.last_time:
                    self.last_time[client] = time.time()
                
                self.batch[client].append((method, body))

                if len(self.batch[client]) >= BATCH_SIZE or (time.time() - self.last_time[client] >= BATCH_TIMEOUT):
                    self.process_message_batch(self.batch[client], self.pipeline)

                    for m, _ in self.batch[client]:
                        ch.basic_ack(delivery_tag=m.delivery_tag)
                    self.batch[client] = []
                    self.last_time[client] = time.time()
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def load_custom_state(self, data):
        # no persiste estado
        pass
    def persist_state(self):
        # no persiste estado
        pass

    def clean_batch(self, client, ch):
        if client in self.batch:
            for m, _ in self.batch[client]:
                ch.basic_ack(delivery_tag=m.delivery_tag)
        

if __name__ == '__main__':
    OverviewProcessor()