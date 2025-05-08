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
            row_str = f"{country_name}{constants.SEPARATOR}{budget}{constants.SEPARATOR}{client}"
            key_from_country = self.number_from_country(country_name)
            return str(key_from_country), row_str
        return None, None
    
    def number_from_country(self, country_name):
        number = sum(
            ord(char) for char in country_name
        )
        return number % 10

if __name__ == '__main__':
    FilterOneProd()