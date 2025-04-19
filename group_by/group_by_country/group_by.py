from queue_manager.queue_manager import QueueManagerConsumer, QueueManagerPublisher
import constants

queue_manager_input = QueueManagerConsumer()
queue_manager_input.declare_exchange(exchange_name='filter_one_prod', exchange_type='direct')
queue_name = queue_manager_input.queue_declare(queue_name='')

queue_manager_output = QueueManagerPublisher()
queue_manager_output.queue_declare(queue_name='group_by_country', exclusive=False)

binds = [str(i) for i in range(10)] + ["-1"]

for bind in binds:
    queue_manager_input.queue_bind(
        exchange_name='filter_one_prod', queue_name=queue_name, routing_key=bind)
    print(f" [*] Waiting for logs. To exit press CTRL+C: {bind}")

results = {}

def callback(_ch, method, _properties, body):
    if method.routing_key == "-1" and body.decode() == constants.END:  # La segunda de las condiciones puede ser redundante
        print(" [*] Received EOF for all movies, exiting...")
        queue_manager_input.stop_consuming()
        queue_manager_input.close_connection()
        for country, budget in results.items():
            row_str = f"{country}{constants.SEPARATOR}{budget}"
            queue_manager_output.publish_message(exchange_name='', routing_key='group_by_country', message=row_str)
        queue_manager_output.publish_message(exchange_name='', routing_key='group_by_country', message=constants.END)
        queue_manager_output.close_connection()
        return
    
    body_split = body.decode().split(constants.SEPARATOR)
    country_name = body_split[0]
    budget = body_split[1]
    if country_name not in results:
        results[country_name] = 0
    results[country_name] += int(budget)    

        


queue_manager_input.consume_messages(
    queue_name=queue_name,
    callback=callback
)
queue_manager_input.start_consuming()
