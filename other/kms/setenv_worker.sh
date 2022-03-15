input=".env"
while IFS= read -r line
do
  export "$line"
done < "$input"

airflow selery worker