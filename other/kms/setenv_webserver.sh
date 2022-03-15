python env_yaml_to_dotenv.py

input=".env"
while IFS= read -r line
do
  export "$line"
done < "$input"

airflow webserver -p $PORT -w 1