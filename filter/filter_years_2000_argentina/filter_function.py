import constants

def filter_function(body_split):
    if int(body_split[1].split("-")[0]) >= 2000:
        movie_id = body_split[0]
        title = body_split[2]
        row_str = f"{movie_id}{constants.SEPARATOR}{title}"
        return str(movie_id[-1]), row_str
    return None, None
