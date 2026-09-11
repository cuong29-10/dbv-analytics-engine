# Hugging Face Spaces chạy container này; app tự đọc PORT nên chỉ cần đặt đúng cổng Spaces mong đợi.
FROM python:3.13-slim

RUN useradd -m -u 1000 app
WORKDIR /home/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /home/app/data && chown -R app:app /home/app
USER app

ENV PORT=7860 \
    DBV_DATA_DIR=/home/app/data
EXPOSE 7860

CMD ["python", "server.py"]
