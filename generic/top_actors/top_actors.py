import constants
import node
import os
from generic import Generic

class TopActors(Generic):
    def __init__(self):
        self.ocurrences = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode() == constants.END:
            print(f" [*] Received EOF for bind {method.routing_key}")
            self.node_instance.send_end_message(method.routing_key)
            self.ended += 1
            if self.ended == self.node_instance.total_binds():
                print(" [*] Received EOF for all, exiting...")
                self.node_instance.stop_consuming_and_close_connection(0)
                top_ten = sorted(
                    self.ocurrences.items(),
                    key=lambda x: (-x[1][0], x[1][1])
                )[:10]
                for id, (count, name) in top_ten:
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 4 -> {id} {count} {name}"
                    )
                self.node_instance.send_end_message('results')
                self.node_instance.close_publisher_connection()
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            id = body_split[0]
            name = body_split[1]
            if id not in self.ocurrences:
                self.ocurrences[id] = [1, name]
            else:
                self.ocurrences[id][0] += 1

if __name__ == '__main__':
    TopActors()