PROJECT_ID="bonio-da"
STREAM_ID="release-main-datastream-bigquery"
STREAM_REGION="asia-east1"
SOURCE_DATASET_NAME="L1_Release_Main_Staging"
SOURCE_SCHEMA_NAME="1001_update_server"
DEST_DATASET_NAME="L1_Release_Main_Binlog"
MODE="full"
LOG_FILE_PATH="./migration_log.txt"
found_last_migrate_table=false

table_list=($(bq ls -n 100000 $PROJECT_ID:$SOURCE_DATASET_NAME | tail -n +3 | awk '{print $1}'))
total_table_count=${#table_list[@]}
echo "total table count: $total_table_count"
migrate_count=0
for table in "${table_list[@]}"
do
  migrate_count=$((migrate_count + 1))
  if [[ "$found_last_migrate_table" == false ]]; then
    # 如果匯入遇到資料表已經在現有環境被刪除，請將該資料表填入以下 if 條件中
    # 如果匯入遇到 BQ API 導致匯入失敗，請將該資料表之前一張填入以下 if 條件中
    if [[ "$table" == "user_feedbacks_log" ]]; then
      found_last_migrate_table=true
    fi
    echo "skip $table"
    continue
  fi
  echo "migrate $table ($migrate_count out of $total_table_count)"
  source_table=${table%_log}
  docker_cmd="docker run -v output:/output -ti --volumes-from gcloud-config \
    migration-service python3 ./migration/migrate_table.py $MODE \
    --force \
    --project-id $PROJECT_ID \
    --stream-id $STREAM_ID \
    --bigquery-destination-dataset-id $DEST_DATASET_NAME \
    --datastream-region $STREAM_REGION \
    --source-schema-name $SOURCE_SCHEMA_NAME \
    --source-table-name $source_table \
    --bigquery-source-dataset-name $SOURCE_DATASET_NAME \
    --bigquery-source-table-name $table"
  
  if ! eval "$docker_cmd" >> "$LOG_FILE_PATH" 2>&1; then
    echo "Docker 指令執行失敗:" | tee -a "$LOG_FILE_PATH"
    echo "$docker_cmd" | tee -a "$LOG_FILE_PATH" 
    exit 1
  fi

  sleep 3
done
