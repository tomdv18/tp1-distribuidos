import constants
import node
import os
from filter import Filter

class FilterSpainArgentina(Filter):
    def filter(self, body_split):
        if "spain" in body_split[4].lower() and "argentina" in body_split[4].lower():
            movie_id = body_split[0]
            genres = body_split[1]
            release_date = body_split[5]
            title = body_split[7]
            client = body_split[8]
            message_id = body_split[9]
        
            row_str = f"{movie_id}{constants.SEPARATOR}{title}{constants.SEPARATOR}{genres}{constants.SEPARATOR}{release_date}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
            return str(movie_id[-1]), row_str
        return None, None

if __name__ == '__main__':
    FilterSpainArgentina()