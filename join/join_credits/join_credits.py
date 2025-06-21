import constants
import node
import os
from join import Join

class JoinCredits(Join):
    def callback_joined(self, _ch, method, _properties, body):
        if body.decode().startswith(constants.END):
            client = body.decode()[len(constants.END):].strip()
            print(f" [*] Received EOF for ratings bind {method.routing_key} from client {client}")
            if client not in self.clients_ended_joined:
                self.clients_ended_joined[client] = 0
            self.clients_ended_joined[client] += 1
            if self.clients_ended_joined[client] == self.node_instance.total_binds():
                print(f" [*] Client {client} finished all credits binds.")
                if client in self.clients_ended_metadata and self.clients_ended_metadata[client] == self.node_instance.total_binds():
                    self.send_pending(client)
                
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            actor_id = body_split[1]
            name = body_split[2]
            client = body_split[3]
            message_id = body_split[4]
            if self.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                return 
            if client not in self.results:
                self.results[client] = {}
            if movie_id in self.results[client]:
                message_id = self.generate_message_id(constants.JOIN_CREDITS)
                row_str = f"{actor_id}{constants.SEPARATOR}{name}{constants.SEPARATOR}{movie_id}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
                self.node_instance.send_message(
                    routing_key=actor_id[-1],
                    message=row_str
                )
            else:
                if client not in self.waiting:
                    self.waiting[client] = {}
                if movie_id not in self.waiting[client]:
                    self.waiting[client][movie_id] = []
                self.waiting[client][movie_id].append((actor_id, name))

    def send_pending(self, client):
        if client in self.finished:
            self.remove_client(client)
            return
        if client not in self.waiting:
            self.node_instance.send_end_message_to_all_binds(client)
            self.finished.append(client)
            self.remove_client(client)
            return
        for movie_id, actors in self.waiting[client].items():
            if movie_id in self.results[client]:
                for actor_id, name in actors:
                    row_str = f"{actor_id}{constants.SEPARATOR}{name}{constants.SEPARATOR}{movie_id}{constants.SEPARATOR}{client}"
                    self.node_instance.send_message(
                        routing_key=actor_id[-1],
                        message=row_str
                    )
        self.node_instance.send_end_message_to_all_binds(client)
        self.finished.append(client)
        self.remove_client(client)

if __name__ == '__main__':
    JoinCredits()