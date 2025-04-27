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
            movie_id = body_split[0]
            budget = body_split[2]
            row_str = f"{country_name}{constants.SEPARATOR}{budget}"
            return str(movie_id[-1]), row_str
        return None, None

if __name__ == '__main__':
    FilterOneProd()