import constants
import node
import os
from join import Join

class JoinCredits(Join):
    def callback_joined(self, _ch, method, _properties, body):
        if body.decode() == constants.END: 
            print(f" [*] Received EOF for ratings bind {method.routing_key}")
            self.ended_joined += 1
            if self.ended_joined == self.node_instance.total_binds():
                self.node_instance.stop_consuming_and_close_connection(1)
                self.node_instance.send_end_message_to_all_binds()
                self.node_instance.close_publisher_connection()
                return
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            actor_id = body_split[1]
            name = body_split[2]
            if movie_id in self.results:
                row_str = f"{actor_id}{constants.SEPARATOR}{name}{constants.SEPARATOR}{movie_id}"
                self.node_instance.send_message(
                    routing_key=actor_id[-1],
                    message=row_str
                )

if __name__ == '__main__':
    JoinCredits()