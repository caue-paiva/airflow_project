#amazon linux 2023 is based on a fedora distro so commands are based on that 
PYTHON_VERSION= "3.9.16"
AIRFLOW_VERSION= "2.8.1"
PROJECT_NAME="crypto_data"
#SCRIPT_DIR=
#PARENT_DIR="${dirname "$script_dir"}"

yes | sudo dnf update #update the system
yes | sudo dnf install python3-pip #install python and git, [ yes | ] answers any prompt to the command with yes 
yes | sudo dnf install git

git clone https://github.com/caue-paiva/airflow_project -b vm_branch
cd airflow_project/
cd vm_setup/
MAIN_WORK_DIR="${pwd}" #main dir we will use for the project, named vm_setup

python3 -m venv airflow_env
source /airflow_env/bin/activate
pip install airflowctl

airflowctl init ${PROJECT_NAME} --airflow-version ${AIRFLOW_VERSION} --python-version ${PYTHON_VERSION} #inits the airflowctl project 
cd ${PROJECT_NAME}
${PROJECT_NAME} build #builds project 
cd ..
airflowctl start ${PROJECT_NAME} --background 

cd ${PROJECT_NAME}/dags/ 
mkdir include #creating folder for python code not used by the dags
cd ..
cd ..

mv binance_api.py crypto_data_etl.py my_airflow_project/dags/include/  #move auxiliary python files to include folder

deactivate #deactivates current env only used to isolated airflowctl installation
source ${MAIN_WORK_DIR}/${PROJECT_NAME}/.venv/bin/pip install -r ${MAIN_WORK_DIR}/_requirements.txt

#------ setting up airflow variables and connections, needs to be done in a airflowctl project dir

#--airflow env variables

#13140 hours is equivalent to 1,5 years
airflowctl airflow variables set --description "how far back will data be collected in hours, equivalent to 2,75 years" MAX_TIME_FRAME_HOURS  13140
airflowctl airflow variables set --description "how many hours are there to be between each daily update of the dataset " HOURS_BETWEEN_DAILY_UPDATES  12
airflowctl airflow variables set --description "ow many minutes of data does each row represent" MINS_PER_ROW  5

#--connections

# 1} export all connections to a file using airflowctl airflow connections export ${file}

# 2} Get the specific AWS connection {DO NOT SHARE IT ON GITHUB} and make a json file only with it {or keep the others ununsed connections}

# 3} Import the connections file using airflowctl airflow connections import ${file}
