from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class DashboardBase(BaseModel):
    name: str
    noofdb: int
    patientfile: str
    id: Optional[str]
    graphs: Optional[list] 
    email: Optional[str]

class DashboardCreate(DashboardBase):
    pass

class DashboardInDB(DashboardBase):
    id: Optional[str] = Field(alias="id")
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True
        allow_population_by_field_name = True
