import constants
import node
import os

class Generic:
    def __init__(self):
        self.clients_ended = {}
        self.messages_sended = 0
        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback),
            ],
            node_id = os.getenv("NODE_ID", ""),
        )
        self.node_instance.start_consuming()

    def callback(self, ch, method, _properties, body):
        raise NotImplementedError("Subclass responsibility")
    
    def generate_message_id(self,prefix):
        #RETORNA UN STRING UNICO PARA CADA MENSAJE CON EL SIGUIENTE FORMATO PP-XXXXXXX
        id = f"{prefix}-{self.messages_sended:07d}"
        self.messages_sended += 1
        return id

if __name__ == '__main__':
    Generic()