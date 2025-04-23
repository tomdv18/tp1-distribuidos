import constants

def filter_function(body_split):
    if "argentina" in body_split[4].lower():
        movie_id = body_split[0]
        release_date = body_split[5]
        title = body_split[7]
        row_str = f"{movie_id}{constants.SEPARATOR}{release_date}{constants.SEPARATOR}{title}"
        return str(movie_id[-1]), row_str
    return None, None