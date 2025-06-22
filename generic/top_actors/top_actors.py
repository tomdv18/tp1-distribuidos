import constants
import node
import os
from generic import Generic

class TopActors(Generic):
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
                    message_id = self.generate_message_id(constants.TOP_ACTORS)
                    self.node_instance.send_message(
                        routing_key=str(id[-1]),
                        message=f"{id}{constants.SEPARATOR}{count}{constants.SEPARATOR}{name}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                    )

                self.node_instance.send_end_message_to_all_binds(client)
                self.ocurrences.pop(client, None)
                self.clients_ended.pop(client, None)
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            id = body_split[0]
            name = body_split[1]
            client = body_split[3]
            message_id = body_split[4]
            if self.node_instance.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                return 
            if client not in self.ocurrences:
                self.ocurrences[client] = {}
            if id not in self.ocurrences[client]:
                self.ocurrences[client][id] = [1, name]
            else:
                self.ocurrences[client][id][0] += 1

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Top shutdown.")

if __name__ == '__main__':
    TopActors()