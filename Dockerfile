# GitHub: ghcr.io/<repository_owner>/asam-ods-exd-api-pandascsv:latest
# docker build -t ghcr.io/totonga/asam-ods-exd-api-pandascsv:latest .
# docker run --rm -it -v "$(pwd)/data":"$(pwd)/data" -p 50051:50051 ghcr.io/totonga/asam-ods-exd-api-pandascsv:latest
FROM python:3.12-slim
LABEL org.opencontainers.image.source=https://github.com/totonga/asam_ods_exd_api_pandascsv
LABEL org.opencontainers.image.description="ASAM ODS External Data API for CSV files opened with with pandas (*.csv)"
LABEL org.opencontainers.image.licenses=MIT
WORKDIR /app
# Create a non-root user and change ownership of /app
RUN useradd -ms /bin/bash appuser && chown -R appuser /app
# Install required packages
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade pip && pip3 install -r requirements.txt
# Copy ASAM ODS Interface files into the container
# Download from ASAM ODS GitHub repository
ADD https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods.proto /app/
ADD https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods_external_data.proto /app/
# Use protoc to compile stubs in container
RUN python3 -m grpc_tools.protoc -I. --python_out=. ods.proto
RUN python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ods_external_data.proto
# Copy plugin implementation
COPY __init__.py exd_api_server.py external_data_reader.py external_file_data.py ./
USER appuser
# Start server
CMD [ "python3", "exd_api_server.py"]