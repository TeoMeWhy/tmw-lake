# image base
FROM bitnami/spark:3.5.5
 
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir notebooks
RUN chmod 777 notebooks


CMD ["~/.local/bin/jupyter",    \
     "notebook",                \
     "--no-browser",            \
     "--ip=0.0.0.0",            \
     "--port=8888",             \
     "--NotebookApp.token=''",  \
     "--NotebookApp.password=''"\
     ]
 
