import constants
import node
import os

class Join:
    def __init__(self):
        self.ended_metadata = 0
        self.ended_joined = 0
        self.results = {}

        self.node_instance = node.Node(
            publisher_exchange = os.getenv("PUBLISHER_EXCHANGE", ""),
            binds = os.getenv("BINDS", "").split(",") if os.getenv("BINDS", "") else [],
            consumer_exchanges_and_callbacks=[
                (os.getenv("CONSUMER_EXCHANGE_METADATA", ""), self.callback_metadata),
                (os.getenv("CONSUMER_EXCHANGE_JOINED", ""), self.callback_credits),
            ]
        )
        self.node_instance.start_consuming()

    def callback_metadata(self, _ch, method, _properties, body):
        if body.decode() == constants.END:  
            print(f" [*] Received EOF for metadata bind {method.routing_key}")
            self.ended_metadata += 1
            if self.ended_metadata == self.node_instance.total_binds():
                print(" [*] Received EOF for all movies")
                self.node_instance.stop_consuming_and_close_connection(0)
                return
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            movie_id = body_split[0]
            title = body_split[1]
            if movie_id not in self.results:
                self.results[movie_id] = (title, 0, 0)


    def callback_ratings(self, _ch, method, _properties, body):
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

    def callback_credits(self, _ch, method, _properties, body):
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
    Join()