from fastapi.exceptions import HTTPException
import magic
from fastapi import FastAPI, UploadFile, status, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
import pandas as pd
from io import BytesIO

from udemy_api import get_popular_courses
from data_cleaning import clean_dataset, convert_df_to_csv_string, read_excel_file
from data_analysis import calculate_certification_counts
from certs_categories import get_certifications_data, get_certifications

app = FastAPI()

cleaned_data = None

EXCEL_MIME_TYPE = [
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ]

class CustomHTTPException(HTTPException):
    def __init__(self, status_code: int = 400, detail: str = None):
        super().__init__(status_code=status_code, detail=detail)

async def get_cleaned_data():
    if cleaned_data is None:
        raise CustomHTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No data was uploaded"
        )

    return cleaned_data

async def get_dataframe_from_csv_file(file: UploadFile):
    csv_content = await file.read()
    csv = BytesIO(csv_content)
    df = pd.read_csv(csv)
    return df

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

# Bar graph (Number of total certifications and number of certifications that matched with courses)
@app.get("/number-of-matched-certifications-graph")
async def graph1(cleaned_data: pd.DataFrame = Depends(get_cleaned_data)):
    certifications = list(cleaned_data['certification'].unique())

    cert_match_count = calculate_certification_counts(certifications)
    cert_total_count = len(certifications)

    graphData = {
        'group': ['Certificaciones Totales', 'Certificaciones con Match en Cursos'],
        'cantidad': [cert_total_count, len(cert_match_count)]
    }

    dataF = pd.DataFrame(graphData)
    dataF = dataF.to_dict(orient='records')
    
    return dataF

# Bar graph (Top 10 certifications with more matches in courses)
@app.post("/top-certifications-graph")
async def graph2(dataset: UploadFile | None = None):
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )
    
    df = await get_dataframe_from_csv_file(dataset)

    certifications = list(df['certification'].unique())

    cert_match_count = calculate_certification_counts(certifications)
    cert_match_count = pd.Series(cert_match_count)
    cert_match_count = cert_match_count.sort_values(ascending=False)

    top = 10
    topcerts = cert_match_count.head(top)

    index = topcerts.index.tolist()
    values = topcerts.values.tolist()

    response_payload = []

    for i in range(top):
        response_payload.append({
            'group': index[i],
            'value': values[i]
        })

    return JSONResponse(status_code=status.HTTP_200_OK,
        content=response_payload)

# Bar graph (Top 10 most popular courses)
@app.get("/top-courses-graph")
async def graph3(cleaned_data: pd.DataFrame = Depends(get_cleaned_data)):
    ucourses = get_popular_courses()
    top = 10

    ucourses[1] = ucourses[1].astype(int)
    ucourses = ucourses.drop_duplicates(subset=[0])
    ucourses = ucourses.sort_values(by=[1], ascending=False)
    
    topcourses = ucourses.head(top)
    dataF = []

    for i in range(top):
        dataF.append({
            'group': topcourses.iloc[i][0],
            'value': int(topcourses.iloc[i][1])
        })
    
    return dataF

# Bar graph (Number of certifications over the years)
@app.post("/over-the-years-graph")
async def graph4(dataset: UploadFile | None = None):
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    df = await get_dataframe_from_csv_file(dataset)

    certifications = (
        df[['certification', 'issue_date']]
        .assign(issue_date=lambda df: pd.to_datetime(df['issue_date']).dt.year)
        .groupby(['issue_date'])
        .count()
        .reset_index()
    )

    cert = certifications['certification'].to_list()
    date = certifications['issue_date'].to_list()

    response_payload = []
    for i in range(len(cert)):
        response_payload.append({
            'group': date[i],
            'value': cert[i]
        })
    
    return JSONResponse(status_code=status.HTTP_200_OK,
        content=response_payload)

# Radar Graph (IBM categorized certifications) 
@app.post("/graphs/certifications-categorized")
async def get_certifications_categorized(dataset: UploadFile | None = None):
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )
    
    df = await get_dataframe_from_csv_file(dataset)

    certifications = list(df['certification'].unique())
    response_payload = get_certifications_data(certifications)

    return response_payload



# Radar Graph (IBM categorized certifications by employees uid)
@app.post("/{uid}-categorized-certifications-graph")
async def graph6(uid: str, dataset: UploadFile | None = None): 
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No file was uploaded"
        )

    df = await get_dataframe_from_csv_file(dataset)

    certifications = get_certifications(df, uid)
    response_payload = get_certifications_data(certifications, uid)

    return JSONResponse(status_code=status.HTTP_200_OK,
        content=response_payload)