# Patient Data Analysis and Real-time Monitoring Backend

This repository contains the backend service for a comprehensive patient data analysis and real-time monitoring platform. Built with FastAPI, it provides a robust API for user management, historical data analysis from `.mat` files, and a live data pipeline utilizing OPC UA, Apache Kafka, and WebSockets for real-time vitals monitoring and alerting.

The system is designed to be scalable, handling both large historical datasets and high-frequency live data streams efficiently.

## Core Features

*   **Authentication**: Secure JWT-based authentication for users and administrators with different access levels.
*   **User Management**: Registration with OTP email verification, and an admin panel for user approval, access control, and barring.
*   **Historical Data Processing**: Upload `.mat` patient files, which are automatically converted to the efficient Zarr format and stored in Firebase Storage.
*   **Data Retrieval & Analysis**: Endpoints to fetch historical data with support for server-side filtering (Savitzky-Golay, Butterworth bandpass) and aggregation (min, max, mean, etc.).
*   **Real-time Data Pipeline**:
    *   An OPC UA server simulates live patient vitals.
    *   A Kafka producer consumes data from the OPC UA server and publishes it to Kafka topics.
    *   Kafka consumers process the stream for real-time alerting and persistent storage in Firestore.
*   **Live Monitoring & Alerts**:
    *   Users can configure custom alarms based on vital signs (e.g., HeartRate > 140).
    *   The system evaluates live data against these alarms and triggers email notifications when conditions are met.
    *   Live data is streamed to the frontend via WebSockets.
*   **Dashboard & File Management**: APIs to create, save, and manage user dashboards and control access to patient data files.

## System Architecture

The backend is composed of two primary data flow pipelines: one for historical data and one for real-time data.

### 1. Real-time Data Pipeline

This pipeline is designed for low-latency processing of live patient vitals.

1.  **Data Source**: `opcua_server.py` acts as a simulator, generating live vital signs (HeartRate, CO2, etc.) for one or more patients.
2.  **Ingestion**: `kafkapublisher.py` connects to the OPC UA server, reads the vitals at a regular interval, and publishes them to two Kafka topics:
    *   `vitals_points`: A stream of individual data points for immediate processing.
    *   `vitals_data`: A stream of batched data points for efficient storage.
3.  **Alerting**: `alarmservice.py` consumes from the `vitals_points` topic. It evaluates each data point against user-defined alarms stored in Firestore. If an alarm condition is met, it sends an email alert and records the event.
4.  **Storage**: `livedata.py` consumes from the `vitals_data` topic and writes the batches to a `live_data` collection in Firestore. This data is used to populate live dashboards.
5.  **Visualization**: WebSockets are used to push live data updates from Firestore to connected frontend clients.

### 2. Historical Data Management

This pipeline handles the upload, processing, and retrieval of large patient data files.

1.  **Upload**: An administrator uploads a patient's data as a `.mat` file via the `/upload-patient-file` endpoint.
2.  **Conversion**: The backend converts the `.mat` file into a chunked, compressed Zarr directory. This format is optimized for web-based partial reads of large datasets. The conversion progress is reported to the client via WebSockets.
3.  **Storage**: The resulting Zarr directory is uploaded to Firebase Cloud Storage. Metadata about the file, including its storage path and access permissions, is saved in Firestore.
4.  **Retrieval**: The `/get-data` endpoint allows clients to request specific data slices (vitals, time ranges). The backend reads only the necessary chunks from the Zarr store in Firebase Storage, applies any requested filters, and returns the data.

## Technology Stack

*   **Framework**: FastAPI
*   **Database**: Firebase Firestore
*   **File Storage**: Firebase Cloud Storage
*   **Messaging/Streaming**: Apache Kafka, Zookeeper
*   **Caching**: Redis
*   **Real-time Communication**: WebSockets
*   **Data Formats**: Zarr, MAT
*   **Authentication**: JWT (JSON Web Tokens)
*   **Containerization**: Docker, Docker Compose

## Setup and Installation

Follow these steps to get the backend services running locally.

### 1. Prerequisites

*   Docker and Docker Compose
*   Python 3.10+ and `pip`

### 2. Clone the Repository

```bash
git clone https://github.com/tejas2609/6200backend.git
cd 6200backend
```

### 3. Configure Environment

1.  **Firebase Credentials**: Obtain a Firebase service account key (`.json` file) for your project and place it in the `app/` directory with the filename `firebase-key.json`.

2.  **Environment File**: Create a `.env` file in the `app/` directory by copying the structure from the provided `app/.env` content. Update the values, especially your `MAIL_USERNAME` and `MAIL_PASSWORD` for sending email alerts and OTPs.

    ```dotenv
    MAIL_USERNAME="your-email@gmail.com"
    MAIL_PASSWORD="your-app-password"
    MAIL_FROM="your-email@gmail.com"
    MAIL_PORT=587
    MAIL_SERVER="smtp.gmail.com"
    MAIL_TLS=True
    MAIL_SSL=False
    USE_CREDENTIALS=True
    GOOGLE_APPLICATION_CREDENTIALS="app/firebase-key.json"
    DOWNLOAD_FOLDER="./tmp" # A temporary folder for file processing
    ```
    *Note: For `MAIL_PASSWORD`, use a Google App Password, not your regular Gmail password.*

### 4. Install Dependencies

Install the required Python packages.

```bash
pip install "fastapi[all]" firebase-admin gcsfs zarr scipy kafka-python redis python-jose[cryptography] apscheduler fastapi-mail opcua-client
```

### 5. Start Services

1.  **Start Infrastructure**: Run the `docker-compose.yaml` file to start Zookeeper, Kafka, and Redis containers in the background.

    ```bash
    docker compose up -d
    ```

2.  **Start OPC-UA Server**: In a separate terminal, run the OPC UA data simulator. This will start generating live patient data.

    ```bash
    python app/services/opcua_server.py
    ```

3.  **Run the Main Application**: In another terminal, start the FastAPI server. The Kafka producer and consumers will start automatically.

    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    ```

The backend API is now running and available at `http://localhost:8000`.
