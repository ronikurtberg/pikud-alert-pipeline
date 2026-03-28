FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pikud.py dashboard.py config.py ./
COPY dashboard_app/ dashboard_app/
COPY templates/ templates/
COPY static/ static/
EXPOSE 5000
ENV FLASK_ENV=production
CMD ["python3", "dashboard.py"]
