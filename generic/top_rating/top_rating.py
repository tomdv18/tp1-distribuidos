import constants
import node
import os
from generic import Generic

class TopRating(Generic):
    def __init__(self):
        self.top_rating = None
        self.worst_rating = None
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode() == constants.END:
            print(f" [*] Received EOF for bind {method.routing_key}")
            self.ended += 1
            if self.ended == self.node_instance.total_binds():
                print(" [*] Received EOF for all, exiting...")
                self.node_instance.stop_consuming_and_close_connection(0)
                if self.top_rating is not None:
                    
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 3 -> {self.top_rating[0]} {self.top_rating[1]} {self.top_rating[2]}"
                    )
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 3 -> {self.worst_rating[0]} {self.worst_rating[1]} {self.worst_rating[2]}"
                    )
                self.node_instance.send_end_message('results')
                self.node_instance.close_publisher_connection()
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            rating = float(body_split[2])
        
            if self.top_rating is None or rating > self.top_rating[2]:
                self.top_rating = (movie_id, title, rating)
            if self.worst_rating is None or rating < self.worst_rating[2]:
               self.worst_rating = (movie_id, title, rating)

if __name__ == '__main__':
    TopRating()