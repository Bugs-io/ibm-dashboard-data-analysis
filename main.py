from fastapi.exceptions import HTTPException
import magic
from io import BytesIO
from fastapi import FastAPI, UploadFile, status, Form, Depends
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
from udemy_api import get_popular_courses
from data_processing import clean_dataset, convert_df_to_csv_string, \
    read_excel_file, get_most_attended_certifications, \
    calculate_certification_counts

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


@app.post("/graphs/query-matched-certifications")
async def query_matched_certifications(dataset: UploadFile | None = None):
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    df = await get_dataframe_from_csv_file(dataset)

    certifications = list(df['certification'].unique())

    cert_match_count = calculate_certification_counts(certifications)
    cert_total_count = len(certifications)

    response_payload = {
            "total_certifications_analysed": cert_total_count,
            "number_of_matched_certifications": cert_match_count,
            "number_of_unmatched_certifications": (cert_total_count
                                                   - cert_match_count)
            }

    return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=response_payload
            )


# Bar graph (Top 10 most popular courses)
@app.get("/graphs/top-industry-courses")
async def query_top_industry_courses():
    ucourses = get_popular_courses()
    top = 10

    ucourses[1] = ucourses[1].astype(int)
    ucourses = ucourses.drop_duplicates(subset=[0])
    ucourses = ucourses.sort_values(by=[1], ascending=False)
    
    topcourses = ucourses.head(top)
    response_payload = []

    for i in range(top):
        response_payload.append({
            'group': topcourses.iloc[i][0],
            'value': int(topcourses.iloc[i][1])
        })
    
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=response_payload
    )