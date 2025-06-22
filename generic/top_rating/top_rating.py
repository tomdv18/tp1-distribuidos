import constants
import node
import os
from generic import Generic

class TopRating(Generic):
    def __init__(self):
        self.top_rating = {}
        self.worst_rating = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for bind {method.routing_key} from client {client}")
            if client not in self.clients_ended:
                self.clients_ended[client] = 0
            self.clients_ended[client] += 1
            if self.clients_ended[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all binds.")
                if client in self.top_rating and client in self.worst_rating:
                    message_id = self.generate_message_id(constants.TOP_RATING)
                    self.node_instance.send_message(
                        routing_key= str(self.top_rating[client][0][-1]),
                        message=f"{self.top_rating[client][0]}{constants.SEPARATOR}{self.top_rating[client][1]}{constants.SEPARATOR}{self.top_rating[client][2]}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                    )
                    message_id = self.generate_message_id(constants.TOP_RATING)
                    self.node_instance.send_message(
                        routing_key=str(self.worst_rating[client][0][-1]),
                        message=f"{self.worst_rating[client][0]}{constants.SEPARATOR}{self.worst_rating[client][1]}{constants.SEPARATOR}{self.worst_rating[client][2]}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                    )
                self.node_instance.send_end_message_to_all_binds(client)
                self.top_rating.pop(client, None)
                self.worst_rating.pop(client, None)
                self.clients_ended.pop(client, None)
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            rating = float(body_split[2])
            client = body_split[3]
            message_id = body_split[4]
            if self.node_instance.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 
        
            if client not in self.top_rating or rating > self.top_rating[client][2]:
                self.top_rating[client] = (movie_id, title, rating)
            if client not in self.worst_rating or rating < self.worst_rating[client][2]:
               self.worst_rating[client] = (movie_id, title, rating)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Top shutdown.")

if __name__ == '__main__':
    TopRating()