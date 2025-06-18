import constants
from generic import Generic

class AggregatorQ3(Generic):
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
                    message_id = self.generate_message_id(constants.AGGREGATOR_Q3)
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 3 -> {self.top_rating[client][0]} {self.top_rating[client][1]} {self.top_rating[client][2]}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                    )
                    message_id = self.generate_message_id(constants.AGGREGATOR_Q3)
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 3 -> {self.worst_rating[client][0]} {self.worst_rating[client][1]} {self.worst_rating[client][2]}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                    )
                self.node_instance.send_end_message('results', client)
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
        
            if client not in self.top_rating or rating > self.top_rating[client][2]:
                self.top_rating[client] = (movie_id, title, rating)
            if client not in self.worst_rating or rating < self.worst_rating[client][2]:
               self.worst_rating[client] = (movie_id, title, rating)

    def shutdown(self):
        self.node_instance.stop_consuming_and_close_connection()
        self.node_instance.close_publisher_connection()
        print(" [*] Aggregator Q3 shutdown.")

if __name__ == '__main__':
    AggregatorQ3()