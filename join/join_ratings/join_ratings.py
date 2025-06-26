import constants
import node
import os
from join import Join
import time

class JoinRatings(Join):
    def callback_joined(self, ch, method, _properties, body):
        if body.decode().startswith(constants.CLIENT_TIMEOUT):
            client = body.decode()[len(constants.CLIENT_TIMEOUT):].strip()
            print(f" [*] Received timeout for client {client}")

            if client not in self.clients_timeout:
                self.clients_timeout[client] = time.time()
                self.persist_timeout()

            if client in self.clients_ended_joined:
                print(f" [*] Removing client {client} from EOF list due to timeout.")
                self.clients_ended_joined.pop(client, None)
                self.persist_eof()

            state_changed = False
            if client in self.clients_ended_metadata:
                print(f" [*] Removing client {client} from metadata binds due to timeout.")
                self.clients_ended_metadata.pop(client, None)
                state_changed = True

            if client in self.clients_ended_joined:
                print(f" [*] Removing client {client} from joined binds due to timeout.")
                self.clients_ended_joined.pop(client, None)
                state_changed = True
            
            if client in self.results:
                print(f" [*] Removing client {client} from results due to timeout.")
                self.results.pop(client, None)
                state_changed = True

            if client in self.waiting:
                print(f" [*] Removing client {client} from waiting due to timeout.")
                self.waiting.pop(client, None)
                state_changed = True

            if state_changed:
                self.persist_state() ## IMPLEMENTAR

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
            
            print(f" [*] Received EOF for ratings bind {method.routing_key} from client {client}")
            if client not in self.clients_ended_joined:
                self.clients_ended_joined[client] = []

            if method.routing_key in self.clients_ended_joined[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended_joined[client].append(method.routing_key)
            if len(self.clients_ended_joined[client]) == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all ratings binds.")
                if client in self.clients_ended_metadata and len(self.clients_ended_metadata[client]) == self.node_instance.total_binds():
                    self.send_pending(client)
                
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            rating = body_split[1]
            client = body_split[2]

            if not self.should_process(client):
                print(f" [*] Ignoring message from client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
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

            if node_id not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[node_id] = {}
            self.node_instance.last_message_id[node_id][client] = body_split[-2]

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