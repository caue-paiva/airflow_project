# by default user-data scripts only run when an EC2 instance is started for the first time, its possible to change that but it requires re-starting an instance, so that isnt a priority currently
## for more info https://repost.aws/pt/knowledge-center/execute-user-data-ec2 


#amazon linux 2023 is based on a fedora distro so commands are based on that 
# OBS: remenber to open up a custom TCP port with all IPs allowed at port 8080 on the EC2 instance to connect to the airflow UI remotely  
export USER="ec2-user" #we need to execute commands that create files as the user and not root to avoid ownership and acess issues
export HOME_="/home/"${USER} #because user data scripts on EC2 run as root user, we need to setup a var that makes sure everything we do is in the ec2-user home
export PYTHON_VERSION="3.9.16"
export AIRFLOW_VERSION="2.8.1"
export PROJECT_NAME="crypto_data"
export VENV_NAME="airflow_env"
export MAIN_GIT_FOLDER=${HOME_}/"airflow_project"
export MAIN_WORK_DIR=${MAIN_GIT_FOLDER}/vm_setup/ #main dir we will use for the project, named vm_setup

yes | sudo dnf update #update the system
sudo -u ${USER} touch ${HOME_}/vm_start_logs.log

function start_vm_anew {  #in case the VM is brand new and has no data, so we must setup it from start
  yes | sudo dnf install python3-pip #install python and git, [ yes | ] answers any prompt to the command with yes 
  yes | sudo dnf install git
  
  sudo -u ${USER} git clone https://github.com/caue-paiva/airflow_project -b vm_branch
  cd ${MAIN_WORK_DIR}
  
  sudo -u ${USER} python3 -m venv ${VENV_NAME}
  source ${MAIN_WORK_DIR}/${VENV_NAME}/bin/activate #we will need to activate this env to use the airflowctl command
  sudo -u ${USER} pip install -r _requirements.txt
  export VENV_PATH=${MAIN_WORK_DIR}/${VENV_NAME}
  
  sudo -u ${USER} airflowctl init ${PROJECT_NAME} --airflow-version ${AIRFLOW_VERSION} --python-version ${PYTHON_VERSION} ----venv_path ${VENV_PATH} #inits the airflowctl project 
  cd ${PROJECT_NAME}
  sudo -u ${USER} airflowctl build #builds project 
  cd ..
  sudo -u ${USER} airflowctl start ${PROJECT_NAME} --background 
  
  cd ${PROJECT_NAME}/dags/ 
  sudo -u ${USER} mkdir include #creating folder for python code not used by the dags
  cd ..
  cd ..
  
  mv binance_api.py crypto_data_etl.py ${PROJECT_NAME}/dags/include/  #move auxiliary python files to include folder
  mv crypto_data.py ${PROJECT_NAME}/dags/ #move main dag file to dag folder
  mv variables_setup.json ${PROJECT_NAME}/  #moves json file that setups the env variables to inside the project
  mv aws_connection_setup.json ${PROJECT_NAME}/
  
  cd ${PROJECT_NAME}
  sudo -u ${USER} airflowctl airflow variables import variables_setup.json #setting up airflow env variables
  if [ $? == 0 ]; then
      echo "Start and setup sucessful" >> ${HOME_}/vm_start_logs.log
  else
      echo "Start and setup  unsucessful" >> ${HOME_}/vm_start_logs.log
  fi

  # now its time to edit the aws_connection_setup.json file with your aws account/IAM identity acess key ID and secret acess keys
  # then run the command (inside the airflowctl project folder): 
  # airflowctl airflow connection import aws_connection_setup.json
  
  #13140 hours is equivalent to 1,5 years
  
  #--connections
  
  # 1} export all connections to a file using airflowctl airflow connections export ${file}, needs to be inside airflowctl project folder
  
  # 2} Get the specific AWS connection {DO NOT SHARE IT ON GITHUB} and make a json file only with it {or keep the others ununsed connections}
  
  # 3} Import the connections file using airflowctl airflow connections import ${file}
  
  # You can setup the connections on airflow UI, but remenber to not only put the JSON but also to fill the bars with the keys above the extra camp
}

function restart_airflow { #in case the VM already has the files, useful for AWS academy labs re-starting
  cd ${MAIN_WORK_DIR}
  source ${MAIN_WORK_DIR}/${VENV_NAME}/bin/activate
  airflowctl start ${PROJECT_NAME}/ --background
  if [ $? == 0 ]; then
      echo "Restart sucessful" >> ${HOME_}/vm_start_logs.log
  else
      echo "Restart unsucessful" >> ${HOME_}/vm_start_logs.log
  fi
}

if [ -d "${MAIN_GIT_FOLDER}" ]; then
  sudo -u ${USER} echo "Directory exists." >> ${HOME_}/vm_start_logs.log
  restart_airflow #runs restart func
else
  sudo -u ${USER} echo "Directory does not exist." >> ${HOME_}/vm_start_logs.log
  start_vm_anew  #runs setting up from scratch func
fi
