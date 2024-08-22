from pydantic import BaseModel, validator, ValidationError
from datetime import datetime

class EmployeeModel(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

    @validator('datetime')
    def validate_datetime(cls, v):
        try:
            # Replace 'Z' with '+00:00' for UTC
            if v.endswith('Z'):
                v = v[:-1] + '+00:00'
            result = datetime.fromisoformat(v)
            print("Validation successful:", result)
            return result
        except ValueError:
            raise ValueError('datetime must be in ISO 8601 format')

class DepartmentModel(BaseModel):
    id: int
    department: str

class JobModel(BaseModel):
    id: int
    job: str
