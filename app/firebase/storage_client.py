from firebase_admin import storage
import uuid

bucket = storage.bucket()

def upload_file(file_obj, filename: str, content_type: str):
    unique_id = str(uuid.uuid4())
    blob = bucket.blob(f"uploads/{unique_id}_{filename}")
    blob.upload_from_file(file_obj, content_type=content_type)
    blob.make_public()  # Optional
    return blob.public_url
