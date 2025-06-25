import constants
import node
import os
from join import Join

class JoinRatings(Join):
    def callback_joined(self, ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for ratings bind {method.routing_key} from client {client}")
            if client not in self.clients_ended_joined:
                self.clients_ended_joined[client] = 0
            self.clients_ended_joined[client] += 1
            if self.clients_ended_joined[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all ratings binds.")
                if client in self.clients_ended_metadata and self.clients_ended_metadata[client] == self.node_instance.total_binds():
                    self.send_pending(client)
                
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            rating = body_split[1]
            client = body_split[2]
            message_id = body_split[3]
            node_id = body_split[4]
            if self.node_instance.is_repeated(message_id, client, node_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return 

            if client not in self.results:
                self.results[client] = {}
            if movie_id in self.results[client]:
                title, count, total, message_id = self.results[client][movie_id]
                self.results[client][movie_id] = (title, count + 1, total + float(rating), message_id)
            else:
                if client not in self.waiting:
                    self.waiting[client] = {}
                if movie_id not in self.waiting[client]:
                    self.waiting[client][movie_id] = []
                self.waiting[client][movie_id].append(rating)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_pending(self, client):
        if client in self.finished:
            self.remove_client(client)
            return
        for movie_id, ratings in self.waiting[client].items():
            if movie_id in self.results[client]:
                title, count, total, message_id = self.results[client][movie_id]
                for rating in ratings:
                    total += float(rating)
                    count += 1
                self.results[client][movie_id] = (title, count, total, message_id)
        for movie_id, (title, count, total, message_id) in self.results[client].items():
            if count > 0:
                avg_rating = total / count
                row_str = f"{movie_id}{constants.SEPARATOR}{title}{constants.SEPARATOR}{avg_rating}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
                self.node_instance.send_message(
                    routing_key=movie_id[-1],
                    message=row_str
                )
        self.node_instance.send_end_message_to_all_binds(client)
        self.finished.append(client)
        self.remove_client(client)

if __name__ == '__main__':
    JoinRatings()