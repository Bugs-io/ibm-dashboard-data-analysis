from fastapi.exceptions import HTTPException
import magic
from fastapi import FastAPI, UploadFile, status, HTTPException, Depends
from fastapi.responses import StreamingResponse
import pandas as pd

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

# Gráfica de barras (Número de certificaciones totales y número de certificaciones que hicieron match con cursos)
@app.get("/graph1")
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

# Gráfica de barras (Top 10 certificaciones con más match en cursos)
@app.get("/graph2")
async def graph2(cleaned_data: pd.DataFrame = Depends(get_cleaned_data)):
    certifications = list(cleaned_data['certification'].unique())

    cert_match_count = calculate_certification_counts(certifications)
    cert_match_count = pd.Series(cert_match_count)
    cert_match_count = cert_match_count.sort_values(ascending=False)

    top = 10
    topcerts = cert_match_count.head(top)

    index = topcerts.index.tolist()
    values = topcerts.values.tolist()

    dataF = []

    for i in range(top):
        dataF.append({
            'group': index[i],
            'value': values[i]
        })

    return dataF

# Gráfica de barras (Top 10 cursos más populares)
@app.get("/graph3")
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

# Gráfica de barras (certificaciones a través de los años)
@app.get("/graph4")
async def graph4(cleaned_data: pd.DataFrame = Depends(get_cleaned_data)):
    certifications = (
        cleaned_data[['certification', 'issue_date']]
        .assign(issue_date=lambda df: pd.to_datetime(df['issue_date']).dt.year)
        .groupby(['issue_date'])
        .count()
        .reset_index()
    )

    cert = certifications['certification'].to_list()
    date = certifications['issue_date'].to_list()

    dataF = []
    for i in range(len(cert)):
        dataF.append({
            'group': date[i],
            'value': cert[i]
        })
    
    return dataF

# Gráfica radar (todas las certificaciones de IBM) 
@app.get("/graph5")
async def graph5(cleaned_data: pd.DataFrame = Depends(get_cleaned_data)):
    certifications = list(cleaned_data['certification'].unique())
    dataF = get_certifications_data(certifications)

    return dataF

# Gráfica radar (certificaciones de un usuario)
@app.get("/graph6{uid}")
async def graph6(uid: str, cleaned_data: pd.DataFrame = Depends(get_cleaned_data)): 
    certifications = get_certifications(cleaned_data, uid)
    dataF = get_certifications_data(certifications)

    return dataF

