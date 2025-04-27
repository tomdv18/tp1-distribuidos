import constants
import node
import os
from filter import Filter

class Filter2000s(Filter):
    def filter(self, body_split):
        if int(body_split[1].split("-")[0]) >= 2000:
            movie_id = body_split[0]
            title = body_split[2]
            row_str = f"{movie_id}{constants.SEPARATOR}{title}"
            return str(movie_id[-1]), row_str
        return None, None

if __name__ == '__main__':
    Filter2000s()