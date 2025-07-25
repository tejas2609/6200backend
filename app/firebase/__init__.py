import firebase_admin
from firebase_admin import credentials
import gcsfs

import os

key_path = os.path.join(os.path.dirname(__file__), "../firebase-key.json")


if not firebase_admin._apps:
    cred = credentials.Certificate(os.path.abspath(key_path))
    firebase_admin.initialize_app(cred, {
        'storageBucket': "project-8680797989633263399.firebasestorage.app"
    })

firebase_cred = cred