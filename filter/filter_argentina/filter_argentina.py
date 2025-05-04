import constants
import node
import os
from filter import Filter

class FilterArgentina(Filter):
    def filter(self, body_split):
        if "argentina" in body_split[4].lower():
            movie_id = body_split[0]
            release_date = body_split[5]
            title = body_split[7]
            client = body_split[8]
            row_str = f"{movie_id}{constants.SEPARATOR}{release_date}{constants.SEPARATOR}{title}{constants.SEPARATOR}{client}"
            return str(movie_id[-1]), row_str
        return None, None

if __name__ == '__main__':
    FilterArgentina()