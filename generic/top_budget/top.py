import heapq
from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants


queue_manager_input= QueueManagerConsumer()
queue_manager_input.queue_declare(queue_name='group_by_country', exclusive=False)

queue_manager_output = QueueManagerPublisher()
queue_manager_output.queue_declare(queue_name='results', exclusive=False)

# Use a heap to maintain the top 5 budgets
top_budgets = []

def callback(_ch, method, _properties, body):
    if body.decode() == constants.END:
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager_input.stop_consuming()
        queue_manager_input.close_connection()
        
        for budget, country in sorted(top_budgets, reverse=True):
            row_str = f"Query 2 -> {country} {budget}"
            queue_manager_output.publish_message(exchange_name='', routing_key='results', message=row_str)
        
        queue_manager_output.publish_message(exchange_name='', routing_key='results', message=constants.END)
        queue_manager_output.close_connection()
        return
    
    body_split = body.decode().split(constants.SEPARATOR)
    country_name = body_split[0]
    budget = int(body_split[1])

    heapq.heappush(top_budgets, (budget, country_name))
    if len(top_budgets) > 5:
        heapq.heappop(top_budgets)

queue_manager_input.consume_messages(
    queue_name='group_by_country',
    callback=callback
)
queue_manager_input.start_consuming()
