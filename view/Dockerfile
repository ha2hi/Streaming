# https://docs.streamlit.io/deploy/tutorials/docker
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY view.py view.py

EXPOSE 8501

CMD ["streamlit", "run", "your_streamlit_app.py"]
