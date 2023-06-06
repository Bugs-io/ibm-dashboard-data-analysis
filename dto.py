from pydantic import BaseModel


class MostAttendedCertificationsRequestDTO(BaseModel):
    limit: int = 10
    since_years: int = 5

