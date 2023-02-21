# Example of using this:
#  ```shell
#  sh dbt_docker/dbt_bq_docker.sh \
#      --dir-name my_project \
#      --dataset my_dataset \
#      --profile-name my_profile \
#      --project-id my_project_id
#  ```

#if $1 = "-dbg"; then
set -x
#fi

echo "args: $1 $2 $3 $4 $5 $6 $7 $8"

sh install_by_chatGPT_v2.sh $1 $2 $3 $4 $5 $6 &7 &8