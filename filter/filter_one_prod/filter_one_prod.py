import constants
import node
import os
from filter import Filter

class FilterOneProd(Filter):
    def filter(self, body_split):
        if body_split[4].count(',') == 1:
            try:
                country_name = body_split[4].split("'name': '")[1].split("'")[0]
            except IndexError:
                country_name = body_split[4].split("'name': \"")[1].split('"')[0]
            budget = body_split[2]
            client = body_split[8]
            message_id = body_split[9]
            row_str = f"{country_name}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
            key_from_country = self.number_from_country(country_name)
            return str(key_from_country), row_str
        return None, None
    
    def number_from_country(self, country_name):
        number = sum(
            ord(char) for char in country_name
        )
        return number % 10
    
    def end_when_bind_ends(self, bind, client):
        pass

    def end_when_all_binds_end(self, client):
        self.node_instance.send_end_message("-1", client)
        

if __name__ == '__main__':
    FilterOneProd()