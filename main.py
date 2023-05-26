from fastapi.exceptions import HTTPException
import magic
from fastapi import FastAPI, UploadFile, status, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd

from data_cleaning import clean_dataset, convert_df_to_csv_string, read_excel_file
from data_analysis import calculate_certification_counts

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

@app.get("/graph1")
async def graph1():
    if cleaned_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No data was uploaded"
        )
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

@app.get("/graph2")
async def graph2():
    if cleaned_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No data was uploaded"
        )
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