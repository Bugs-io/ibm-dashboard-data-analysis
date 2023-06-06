from fastapi.exceptions import HTTPException
import magic
from io import BytesIO 
from fastapi import FastAPI, UploadFile, status, HTTPException, Form
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd

from udemy_api import get_popular_courses
from data_processing import clean_dataset, convert_df_to_csv_string, read_excel_file, get_most_attended_certifications
from dto import MostAttendedCertificationsRequestDTO

app = FastAPI()

cleaned_data = None

excluded_words = ['Exam', 'Intermediate', 'Guide', 'Project', 'Level', 'Functional', 'Content', 'Support', 'Professional', 'by', 'Streams', 'Framework', 'Portal', 'Language', '10', '2019', '2020', '2021', '2022', '2023', 'Programming', 'Application', '&', 'Using', 'Working', 'Environment', 'The', 'Way', 'Build', 'Part', 'a', 'an', 'and', 'of', 'the', 'to',
                  'with', 'With', 'Building', 'A', 'Applications', 'App', 'Market', 'Software', 'Advanced', 'Parallel', 'Coding', 'Web', '1', '2', '3', 'Learning', 'Introduction', 'Development', 'in', '-', 'Server', 'Studio', 'Practice', 'your', 'for', 'using', 'from', ' ', '', 'on', 'us', 'Visualization']

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

    udemy_courses = get_popular_courses()


    for ucourse in udemy_courses[0]:
        ucourse_words = ucourse.split(' ')
        for certification in certifications:
            certification_words = certification.split(' ')
            for word in certification_words:
                if word not in excluded_words and word in ucourse_words:

                    if certification not in cert_match_count:
                        cert_match_count.add(certification)

                    if word in word_match_count:
                        word_match_count[word] += 1

                    else:
                        word_match_count[word] = 1

    cert_total_count = len(certifications)

    graphData = {
        'group': ['Certificaciones Totales', 'Certificaciones con Match en Cursos'],
        'cantidad': [cert_total_count, len(cert_match_count)]
    }

    dataF = pd.DataFrame(graphData)
    dataF = dataF.to_dict(orient='records')

    return dataF


async def get_dataframe_from_csv_file(file: UploadFile):
    csv_content = await file.read()
    csv = BytesIO(csv_content)
    df = pd.read_csv(csv)
    return df


@app.post("/graphs/query-most-attended-certifications")
async def query_most_attended_certifications(
        dataset: UploadFile | None = None,
        limit: int = Form(...),
        since_years: int = Form(...),
        ):
    print("the limit is: ", limit, "the since_years is: ", since_years)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    df = await get_dataframe_from_csv_file(dataset)
    most_attended_certifications = get_most_attended_certifications(df, limit, since_years)

    response_payload = {
            "count": len(most_attended_certifications),
            "certifications": []
            }

    for certification in most_attended_certifications:
        response_payload["certifications"].append({
            "name": certification.name,
            "total_attendees": certification.total_attendees
            })

    return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_payload
            )
