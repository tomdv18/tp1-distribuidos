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
            client = body_split[4]
            message_id = body_split[5]
            if self.is_repeated(message_id):
                print(f" [*] Repeated message {message_id} from client {client}. Ignoring.")
                return 
            row_str = f"Query 1 -> ID: {movie_id} - Title: {title} - Genres: {genres}{constants.SEPARATOR}{client}{constants.SEPARATOR}{message_id}"
            return os.getenv("PUBLISHER_EXCHANGE", ""), row_str
        return None, None
    
    def end_when_bind_ends(self, bind, client):
        pass

    def end_when_all_binds_end(self, client):
        self.node_instance.send_end_message(
            os.getenv("PUBLISHER_EXCHANGE", ""),
            client
        )

if __name__ == '__main__':
    Filter2000s()