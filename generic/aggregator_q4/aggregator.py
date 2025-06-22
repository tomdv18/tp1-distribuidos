import constants
import node
import os
from generic import Generic

class AggregatorQ4(Generic):
    def __init__(self):
        self.ocurrences = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")
            if client not in self.clients_ended:
                self.clients_ended[client] = 0
            self.clients_ended[client] += 1

            if client not in self.ocurrences:
                self.ocurrences[client] = {} 
            
            if self.clients_ended[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                top_ten = sorted(
                    self.ocurrences[client].items(),
                    key=lambda x: (-x[1][0], x[1][1])
                )[:10]
                for id, (count, name) in top_ten:
                    message_id = self.generate_message_id(constants.AGGREGATOR_Q4)
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 4 -> {id} {count} {name}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                    )
                self.node_instance.send_end_message('results', client)
                self.ocurrences.pop(client, None)
                self.clients_ended.pop(client, None)
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            id = body_split[0]
            count = int(body_split[1])
            name = body_split[2]
            client = body_split[3]
            message_id = body_split[4]
            if self.node_instance.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
            if client not in self.ocurrences:
                self.ocurrences[client] = {}
            if id not in self.ocurrences[client]:
                self.ocurrences[client][id] = [count, name]
            else:
                self.ocurrences[client][id][0] += count
        ch.basic_ack(delivery_tag=method.delivery_tag)

        
    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Aggregator Q4 shutdown.")

if __name__ == '__main__':
    AggregatorQ4()