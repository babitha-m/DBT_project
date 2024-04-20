# DBT

## Setting up Spark:
### We use bitnami's prebuilt spark version. We can access the interactive shell by:
```bash
docker run --name spark -it bitnami/spark:latest /bin/bash
```
### This will create a Docker container of the name spark and pull the latest bitnami spark build.

### Navigate to your desired folder, clone the directory and then use the following command to test batch processing:

```bash
spark-submit 1.py
```