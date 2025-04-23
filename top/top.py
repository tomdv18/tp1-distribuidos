import constants
import node
import os

class Top:
    def __init__(self):
        self.ended = 0
        self.top_rating = None
        self.worst_rating = None
        self.ocurrences = {}

        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE", ""), self.callback_top_ten),
            ]
        )
        self.node_instance.start_consuming()

    def callback(self, _ch, method, _properties, body):
        if body.decode() == constants.END:
            print(f" [*] Received EOF for bind {method.routing_key}")
            self.node_instance.send_end_message(method.routing_key)
            self.ended += 1
            if self.ended == self.node_instance.total_binds():
                print(" [*] Received EOF for all, exiting...")
                self.node_instance.stop_consuming_and_close_connection(0)
                self.node_instance.send_message(
                    routing_key='results',
                    message=f"Query 3 -> {self.top_rating[0]} {self.top_rating[1]} {self.top_rating[2]}"
                )
                self.node_instance.send_message(
                    routing_key='results',
                    message=f"Query 3 -> {self.worst_rating[0]} {self.worst_rating[1]} {self.worst_rating[2]}"
                )
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

    def callback_top_ten(self, ch, method, _properties, body):
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
    Top()