import json
import requests
import pandas as pd

def get_popular_courses():
    url = 'https://www.udemy.com/api-2.0/courses/'
    params = {
        'category': 'Development',
        'sort': 'popularity',
        'instructional_level': 'expert',
        'language': 'en',
        'fields[course]': 'title,headline,url,price,primary_category,num_subscribers,avg_rating',
        'page': 1,  # Página a obtener
        'page_size': 10  # Máximo de cursos a obtener por página
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": "Basic YW9NMTNScHRyTXFTOTh4RW4xcWRONFIxWldFTEVrNjBVOTVzUm5NMzpLeUVUUHAyV0dRa2JxTUtPemNwcm96TU56OEdYOFVFOTllcEFveWZYdHRZY1ZIQTJIUTMzdmFDRTFPVVFSZ0QwcElSWkVQbVZsRFZBOXM2N2lxWEJpbDYwbGNMYzZrcUt4TWNpbEhpc1lXS0k5MWpWRURaZlRZbFp1T0RQaGRydQ==",
        "Content-Type": "application/json"
    }

    response = requests.get(url, params=params, headers=headers)
    data = json.loads(response.text)

    popular_courses = []

    # Cambiamos de páginas hasta que no haya más cursos
    while 'next' in data and params['page'] < 21:
        response = requests.get(data['next'], headers=headers)
        data = json.loads(response.text)
        params['page'] += 1

        for course in data['results']:
            if course['num_subscribers'] > 1000:
                popular_courses.append(course['title'])

    udemy_courses = pd.DataFrame(popular_courses)

    return udemy_courses
