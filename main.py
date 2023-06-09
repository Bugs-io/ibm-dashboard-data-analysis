from fastapi.exceptions import HTTPException
import magic
from io import BytesIO
from fastapi import FastAPI, UploadFile, status, Form
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
from udemy_api import get_popular_courses_df
from data_processing import clean_dataset, convert_df_to_csv_string, \
    read_excel_file, get_most_attended_certifications, \
    get_matched_certifications, get_certifications, get_certifications_data, \
    get_certifications_distribution


app = FastAPI()

cleaned_data = None

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

    certification_df = await get_dataframe_from_csv_file(dataset)
    udemy_courses_df = get_popular_courses_df()

    matched_certifications_df = get_matched_certifications(
            certification_df,
            udemy_courses_df)

    cert_match_count = len(matched_certifications_df)
    cert_total_count = len(certification_df['certification'].unique())

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
    ucourses = get_popular_courses_df()
    top = 10

    ucourses["num_subscribers"] = ucourses["num_subscribers"].astype(int)
    ucourses = ucourses.drop_duplicates(subset=["title"])
    ucourses = ucourses.sort_values(by=["num_subscribers"], ascending=False)

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


# Radar Graph (IBM categorized certifications by employees uid)
@app.post("/employees/{employee_id}/certifications-categorized")
async def get_employee_certifications_categorized(
        dataset: UploadFile,
        employee_id: str):
    if not dataset:
        raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No file was uploaded"
                )

    certification_df = await get_dataframe_from_csv_file(dataset)
    certifications = get_certifications(certification_df, employee_id)
    dataF = get_certifications_data(certifications, employee_id)

    return dataF


@app.post("/graphs/over-the-years")
async def over_the_years(dataset: UploadFile | None = None):
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    certifications_df = await get_dataframe_from_csv_file(dataset)
    certifications_df["issue_year"] = pd.to_datetime(certifications_df["issue_date"]).dt.year

    taken_certifications_df = certifications_df[["certification", "issue_year"]]\
        .groupby("issue_year")\
        .count()\
        .reset_index()

    result = []

    for index, row in taken_certifications_df.iterrows():
        result.append({
            "year": int(row["issue_year"]),
            "taken_certifications": int(row["certification"])
        })

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=result
    )


@app.post("/graphs/certifications-categorized")
async def get_certifications_categorized(dataset: UploadFile | None = None):
    df = await get_dataframe_from_csv_file(dataset)

    certifications = list(df['certification'].unique())
    response_payload = get_certifications_data(certifications)

    return response_payload


@app.post("/graphs/certifications-distribution")
async def certifications_distribution(dataset: UploadFile | None = None):
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    certifications_df = await get_dataframe_from_csv_file(dataset)
    certifications = certifications_df["certification"].unique().tolist()

    certifications_distribution = get_certifications_distribution(certifications)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=certifications_distribution
    )
