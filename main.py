from fastapi.exceptions import HTTPException
import magic
from fastapi import FastAPI, UploadFile, status, HTTPException
from fastapi.responses import StreamingResponse
import json
import requests
import pandas as pd
from utils import clean_dataset, convert_df_to_csv_string, read_excel_file

app = FastAPI()

cleaned_data = None

excluded_words = ['Exam', 'Intermediate', 'Guide', 'Project', 'Level', 'Functional', 'Content', 'Support', 'Professional', 'by', 'Streams', 'Framework', 'Portal', 'Language', '10', '2019', '2020', '2021', '2022', '2023', 'Programming', 'Application', '&', 'Using', 'Working', 'Environment', 'The', 'Way', 'Build', 'Part', 'a', 'an', 'and', 'of', 'the', 'to',
                  'with', 'With', 'Building', 'A', 'Applications', 'App', 'Market', 'Software', 'Advanced', 'Parallel', 'Coding', 'Web', '1', '2', '3', 'Learning', 'Introduction', 'Development', 'in', '-', 'Server', 'Studio', 'Practice', 'your', 'for', 'using', 'from', ' ', '', 'on', 'us', 'Visualization']


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




EXCEL_MIME_TYPE = [
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ]

@app.post("/clean-internal-dataset")
async def upload(file: UploadFile | None = None):
    if not file:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    content = await file.read()

    file_type = magic.from_buffer(content, mime=True)

    if file_type not in EXCEL_MIME_TYPE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type is not supported: {file_type}"
        )
    
    df = read_excel_file(content)
    df = clean_dataset(df)
    csv_string = convert_df_to_csv_string(df)

    response = StreamingResponse(
        iter([csv_string]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=clean-data.csv"

    global cleaned_data
    cleaned_data = df

    return response

@app.get("/graph1")
async def graph1():
    if cleaned_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No data was uploaded"
        )
    certifications = list(cleaned_data['certification'].unique())

    cert_match_count = set()
    word_match_count = {}


    for ucourse in udemy_courses[0]:
        ucourse_words = ucourse.split(' ')
        for certification in certifications:
            certification_words = certification.split(' ')
            for word in certification_words:
                if word not in excluded_words and word in ucourse_words:
                    '''print(f"Certification: {certification}\n")
                    print(f"Course: {ucourse}\n")
                    print(f"similarity: -----{word}-----")
                    print('\n')'''
                    # Contar coincidencias por certificación
                    if certification not in cert_match_count:
                        cert_match_count.add(certification)

                    # Contar coincidencias por palabra
                    if word in word_match_count:
                        word_match_count[word] += 1
                    else:
                        word_match_count[word] = 1
        
    cert_total_count = len(certifications)

    dataG = {
        'Categoría': ['Certificaciones Totales', 'Certificaciones con Match en Cursos'],
        'Cantidad': [cert_total_count, len(cert_match_count)]
    }

    dataF = pd.DataFrame(dataG)

    x = []
    for i, value in enumerate(dataF['Cantidad']):
        x.append({
            'group': dataF['Categoría'][i],
            'cantidad': value
        }) 

    
    # Do some analysis on the cleaned data
    return x
