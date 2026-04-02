FROM python:3.11-slim AS runtime
WORKDIR /app
ENV APP_ROLE=bot
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN useradd -m botuser && mkdir -p /app/bot-data && chown -R botuser:botuser /app
USER botuser
CMD ["python", "-m", "app.entrypoint"]
