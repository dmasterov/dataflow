FROM minio/minio:latest

ENV MINIO_ROOT_USER=minioadmin
ENV MINIO_ROOT_PASSWORD=minioadmin

EXPOSE 9000 9001

ENTRYPOINT ["minio", "server", "/data", "--console-address", ":9001"]
#docker build -t my-minio-image .
#docker run -d --name minio -p 9000:9000 -p 9001:9001 my-minio-image

