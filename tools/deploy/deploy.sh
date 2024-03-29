#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
BIN_DIR="${SCRIPT_DIR//tools\/deploy/}bin/"
echo $SCRIPT_DIR
echo $BIN_DIR

#------------- SERVERS FILE FORMAT------------
# 0.0 master_node_ip
# x.y regular_node_ip
# x.z other_regular_node_ip
#---------------------------------------------

#      1           2       3        4
# servers_file username key.pem remote_dir
start_nodes() {
  while read server;
  do
    cleaned_string=$(sed 's/\r$//' <<< $server)
    serverArray=($cleaned_string)
    if [[ "${serverArray[0]}" != "0.0" ]]; then
      echo "Starting node ${serverArray[0]} on ${serverArray[1]}"
      ssh -n -i ${3} $2@${serverArray[1]} "cd ${4}; ./node -id=${serverArray[0]} -log_level=info> log.txt 2>&1 &"
    fi
  done < "${1}"
}

#      1           2       3        4          5
# servers_file username key.pem remote_dir config_file
stop_nodes() {
  while read server;
  do
    cleaned_string=$(sed 's/\r$//' <<< $server)
    serverArray=($cleaned_string)
    if [[ "${serverArray[0]}" == "0.0" ]]; then
      echo "Stopping nodes"
      ssh -n -i ${3} $2@${serverArray[1]} "cd ${4}; ./master -close -config=${5}"
    fi
  done < "${1}"
}

#      1           2       3        4           5
# servers_file username key.pem remote_dir local_config
upload_config() {
  while read server;
  do
    cleaned_string=$(sed 's/\r$//' <<< $server)
    serverArray=($cleaned_string)
    if [[ "${serverArray[0]}" == "0.0" ]]; then
      echo "uploading config"
      upload_file ${2} ${3} ${serverArray[1]} $5 $4
    fi
  done < "${1}"
}

#       1          2       3         4        5           6
# servers_file username key.pem local_dir remote_dir local_config
upload_directory_to_servers() {
  while read server;
  do
    cleaned_string=$(sed 's/\r$//' <<< $server)
    serverArray=($cleaned_string)
    echo "Uploading dir ${4} to ${serverArray[0]} @ ${serverArray[1]}"
    upload_directory ${2} ${3} ${serverArray[1]} $4 $5
    upload_file ${2} ${3} ${serverArray[1]} $6 $5
    echo "done uploading to ${serverArray[0]}"
  done < "${1}"
}

#      1       2        3         4         5
# username key.pem ip_address local_dir remote_dir
upload_directory () {
  echo "removing ${5} from remote ${3}"
	ssh -n -i ${2} $1@$3 "rm -r $5"
  echo "uploading ${4} to ${3}"
	scp -i ${2} -r $4 $1@$3:$5 < /dev/null
}

# username key.pem ip_address local_file remote_file
upload_file () {
  echo "Uploading file ${4} to ${3}"
	scp -i ${2} $4 $1@$3:$5 < /dev/null
}

#     1        2        3          4                5
# username key.pem ip_address remote_source local_destination
download() {
    echo "download from: $2"
    echo "download what: $3"
    echo "download where: $4"
	scp -r -i ${2} $1@$3:$4 $5 < /dev/null
}

#     1        2        3          4                5
# username key.pem ip_address remote_source local_destination
download_file() {
    echo "download from: $2"
    echo "download what: $3"
    echo "download where: $4"
	scp -i ${2} $1@$3:$4 $5 < /dev/null
}

#        1         2       3       4         5
# servers_file username key.pem remote_dir local_dir
download_from_servers() {
  while read server;
  do
    cleaned_string=$(sed 's/\r$//' <<< $server)
    serverArray=($cleaned_string)
    echo "downloading ${4} from ${serverArray[0]} @ ${serverArray[1]} to ${5}"
    download ${2} ${3} ${serverArray[1]} $4 $5
    echo "done downloading from ${serverArray[0]}"
  done < "${1}"
}

#        1         2       3       4         5
# servers_file username key.pem remote_dir local_dir
download_logs_from_servers() {
  while read server;
  do
    cleaned_string=$(sed 's/\r$//' <<< $server)
    serverArray=($cleaned_string)
    echo "downloading logs from ${serverArray[0]} @ ${serverArray[1]} to ${5}"
    download_file ${2} ${3} ${serverArray[1]} $4 $5
    echo "done downloading from ${serverArray[0]}"
  done < "${1}"
}

echo "Running CloudStorm Deploy script"
echo "This script compiles CloudStorm from source and uploads to the list of servers provided"

case $1 in
deploy)
  #     0       1       2      3        4              5              6
  # deploy.sh deploy ubuntu key.pem servers.txt /home/ubuntu/clt config.json
  cd "$BIN_DIR"
  ./build.sh
  cd $SCRIPT_DIR
  upload_directory_to_servers $4 $2 $3 $BIN_DIR $5 $6
  ;;
deploy_config)
  #     0           1          2      3        4              5              6
  # deploy.sh deploy_config ubuntu key.pem servers.txt /home/ubuntu/clt config.json
  upload_config $4 $2 $3 $5 $6
  ;;
start)
   # deploy.sh start ubuntu key.pem servers.txt /home/ubuntu/clt
  start_nodes $4 $2 $3 $5
  ;;
stop)
   # deploy.sh stop ubuntu key.pem servers.txt /home/ubuntu/clt config_aws.json
  stop_nodes $4 $2 $3 $5 $6
  ;;
download_dir)
  #     0           1         2      3        4              5                  6
  # deploy.sh download_dir ubuntu key.pem servers.txt /home/ubuntu/clt /home/aleksey/clt_raw
  download_from_servers $4 $2 $3 $5 $6
  ;;
download_logs)
  #     0                       1           2      3        4              5                  6
  # deploy.sh download_logs_from_servers ubuntu key.pem servers.txt /home/ubuntu/clt /home/aleksey/clt_raw
  download_logs_from_servers $4 $2 $3 $5 $6
  ;;
clean)
  ;;
*)
  echo "Usage:"
  echo "  - $0 deploy username key_file server_list remote_location config_file
   -- compiles and deploys code to the servers in the server_list file. It expects all servers to have username account and be accessible with a key_file"
  echo "  - $0 start username key_file server_list remote_location
      -- starts nodes specified in the server_list file"
  ;;
esac