import constants
import node
import os
import time
from join import Join

class JoinCredits(Join):
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

            if client in self.results:
                print(f" [*] Removing client {client} from results due to timeout.")
                self.results.pop(client, None)
                state_changed = True

            if client in self.waiting:
                print(f" [*] Removing client {client} from waiting due to timeout.")
                self.waiting.pop(client, None)
                state_changed = True

            if state_changed:
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
            
            print(f" [*] Received EOF for ratings bind {method.routing_key} from client {client}")

            if client not in self.clients_ended_joined:
                self.clients_ended_joined[client] = []

            if method.routing_key in self.clients_ended_joined[client]:
                print(f" [!] Duplicate EOF from routing key {method.routing_key} for client {client} — ignored.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            self.clients_ended_joined[client].append(method.routing_key)

            if len(self.clients_ended_joined[client]) == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all credits binds.")
                if client in self.clients_ended_metadata and len(self.clients_ended_metadata[client]) == self.node_instance.total_binds():
                    self.send_pending(client)
                
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            actor_id = body_split[1]
            name = body_split[2]
            client = body_split[3]
            if not self.should_process(client):
                print(f" [*] Ignoring message for client {client} due to timeout.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            message_id = body_split[4]
            node_id = body_split[5]
            if self.node_instance.is_repeated(message_id, client, node_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                ch.basic_ack(delivery_tag=method.delivery_tag)

                return 
            if client not in self.results:
                self.results[client] = {}

            if client not in self.waiting:
                self.waiting[client] = {}
            if movie_id not in self.waiting[client]:
                self.waiting[client][movie_id] = []
            self.waiting[client][movie_id].append((actor_id, name))

            if node_id not in self.node_instance.last_message_id:
                self.node_instance.last_message_id[node_id] = {}
            self.node_instance.last_message_id[node_id][client] = body_split[-2]

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_pending(self, client):
        if client in self.finished:
            self.remove_client(client)
            return
        if client not in self.waiting:
            self.node_instance.send_end_message_to_all_binds(client)
            self.finished.append(client)
            self.remove_client(client)
            return
        message_id = 0
        for movie_id, actors in self.waiting[client].items():
            if movie_id in self.results[client]:
                for actor_id, name in actors:
                    row_str = f"{actor_id}{constants.SEPARATOR}{name}{constants.SEPARATOR}{movie_id}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}{constants.SEPARATOR}{self.node_instance.id()}"
                    self.node_instance.send_message(
                        routing_key=actor_id[-1],
                        message=row_str
                    )
                    message_id += 1
        self.node_instance.send_end_message_to_all_binds(client)
        self.finished.append(client)
        self.remove_client(client)

if __name__ == '__main__':
    JoinCredits()