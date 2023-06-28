install() {

    # echo "Instalação do Container do Minio..."
    # docker run --name minio -d -p 9000:9000 -p 9001:9001 -v "$PWD/datalake:/data" minio/minio server /data --console-address ":9001"

    echo "Instação do Container do Airflow..."
    docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password admin --firstname Danilo --lastname Freitas --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

    echo "Conectando ao Container do Airflow..."
    docker container exec -it -u root airflow bash

    echo "Instalando as bibliotecas dentro do Container"
    pip install pymysql xlrd openpyxl minio pyspark findspark install-jdk
    apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

    # echo "Acesso ao Dashboard do Minio em http://localhost:9001"
  
    echo "Acesso do Airflow UI em http://localhost:8080 para a geração da DAG."  

}

jupyterdev() {

    echo "Acesso ao Notebook Jupyter para desenvolvimento do código PySpark..."
    jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root
    
}