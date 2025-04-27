import constants
import node
import os
from generic import Generic

class TopBudget(Generic):
    def __init__(self):
        self.budgets = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode() == constants.END:
            print(f" [*] Received EOF for bind {method.routing_key}")
            self.ended += 1
            if self.ended == self.node_instance.total_binds():
                print(" [*] Received EOF for all movies, exiting...")
                self.node_instance.stop_consuming_and_close_connection(0)
                top_five = sorted(
                    self.budgets.items(),
                    key=lambda x: (-x[1], x[0])
                )[:5]
                for country, budget in top_five:
                    self.node_instance.send_message(
                        routing_key='results',
                        message=f"Query 2 -> {country} {budget}"
                    )
                self.node_instance.send_end_message('results')
                self.node_instance.close_publisher_connection()
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            country_name = body_split[0]
            budget = int(body_split[1]) 
            if country_name not in self.budgets:
                self.budgets[country_name] = budget
            else:
                self.budgets[country_name] += budget

            

if __name__ == '__main__':
    TopBudget()