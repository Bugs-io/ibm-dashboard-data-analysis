import requests
import pandas as pd

UDEMY_API_URL = "https://www.udemy.com/api-2.0"


def append_to_df(df: pd.DataFrame, new_row: dict) -> pd.DataFrame:
    return pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)


def get_popular_courses_df() -> pd.DataFrame:
    params = {
        "category": "Development",
        "sort": "popularity",
        "instructional_level": "expert",
        "language": "en",
        "fields[course]": "title,num_subscribers",
        "page": 1,
        "page_size": 10
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": "Basic YW9NMTNScHRyTXFTOTh4RW4xcWRONFIxWldFTEVrNjBVOTVzUm5NMzpLeUVUUHAyV0dRa2JxTUtPemNwcm96TU56OEdYOFVFOTllcEFveWZYdHRZY1ZIQTJIUTMzdmFDRTFPVVFSZ0QwcElSWkVQbVZsRFZBOXM2N2lxWEJpbDYwbGNMYzZrcUt4TWNpbEhpc1lXS0k5MWpWRURaZlRZbFp1T0RQaGRydQ==",
        "Content-Type": "application/json"
    }

    response = requests.get(f"{UDEMY_API_URL}/courses/", params=params, headers=headers, timeout=5)
    data = response.json()

    popular_courses_df = pd.DataFrame(columns=["title", "num_subscribers"])

    while "next" in data and params["page"] < 15:
        response = requests.get(data["next"], headers=headers)
        data = response.json()
        params["page"] += 1

        for course in data["results"]:
            if course["num_subscribers"] > 1000:
                new_row = {"title": course["title"], "num_subscribers": course["num_subscribers"]}
                popular_courses_df = append_to_df(popular_courses_df, new_row)

    return popular_courses_df
