import constants
import node
import os
from join import Join

class JoinRatings(Join):
    def callback_joined(self, _ch, method, _properties, body):
        if body.decode() == constants.END: 
            print(f" [*] Received EOF for ratings bind {method.routing_key}")
            self.ended_joined += 1
            if self.ended_joined == self.node_instance.total_binds():
                self.node_instance.stop_consuming_and_close_connection(1)
                for id, results in self.results.items():
                    title, count, total = results
                    if count > 0:
                        avg_rating = total / count
                        row_str = f"{id}{constants.SEPARATOR}{title}{constants.SEPARATOR}{avg_rating}"
                        self.node_instance.send_message(
                            routing_key=id[-1],
                            message=row_str
                        )
                self.node_instance.send_end_message_to_all_binds()
                self.node_instance.close_publisher_connection()
                return
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            rating = body_split[1]
            if movie_id in self.results:
                title, count, total = self.results[movie_id]
                self.results[movie_id] = (title, count + 1, total + float(rating))

if __name__ == '__main__':
    JoinRatings()