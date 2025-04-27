import constants
import node
import os
from filter import Filter

class Filter2000s(Filter):
    def filter(self, body_split):
        year = int(body_split[3].split("-")[0])
        if year >= 2000 and year < 2010:
            movie_id = body_split[0]
            genres = body_split[2]
            title = body_split[1]
            row_str = f"Query 1 -> ID: {movie_id} - Title: {title} - Genres: {genres}"
            return os.getenv("PUBLISHER_EXCHANGE", ""), row_str
        return None, None
    
    def end_when_bind_ends(self, bind):
        pass

    def end_when_all_binds_end(self):
        self.node_instance.send_end_message(
            os.getenv("PUBLISHER_EXCHANGE", "")
        )

if __name__ == '__main__':
    Filter2000s()