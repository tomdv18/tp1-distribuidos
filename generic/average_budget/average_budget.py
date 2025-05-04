import constants
import node
import os
from generic import Generic

class AverageBudget(Generic):
    def __init__(self):
        self.results = {}
        self.cant = {}
        super().__init__()

    def callback(self, ch, method, _properties, body):
        if body.decode() == constants.END:
            print(f" [*] Received EOF for bind {method.routing_key}")
            self.ended += 1
            if self.ended == self.node_instance.total_binds():
                print(" [*] Received EOF for all, exiting...")
                self.node_instance.stop_consuming_and_close_connection()
                for sentiment_label in self.results:
                    if self.cant[sentiment_label] != 0:
                        average = self.results[sentiment_label] / self.cant[sentiment_label]
                        self.node_instance.send_message(
                            routing_key='results',
                            message=f"Query 5 -> {sentiment_label} {average}"
                        )
                self.node_instance.send_end_message('results')
                self.node_instance.close_publisher_connection()
        else:
            body_split = body.decode().split(constants.SEPARATOR)
            budget = body_split[1]
            revenue = body_split[2]
            sentiment_label = body_split[3]
            if float(budget) != 0 and float(revenue) != 0:
                revenue_over_budget = float(revenue) / float(budget) if float(budget) != 0 else print("Budget is zero, cannot calculate ratio")        
                if sentiment_label not in self.results:
                    self.results[sentiment_label] = 0
                self.results[sentiment_label] += revenue_over_budget
                if sentiment_label not in self.cant:
                    self.cant[sentiment_label] = 0
                self.cant[sentiment_label] += 1   
                
if __name__ == '__main__':
    AverageBudget()