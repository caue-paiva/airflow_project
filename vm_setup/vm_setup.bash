#amazon linux 2023 is based on a fedora distro so commands are based on that 
PYTHON_VERSION= "3.10.0"
AIRFLOW_VERSION= "2.8.1"
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
PARENT_DIR="$(dirname "$script_dir")"
PYENV_BASHRC_PATH="$PARENT_DIR/pyenv_bashrc.bash"

sudo yum -y install git gcc zlib-devel bzip2-devel readline-devel sqlite-devel openssl-devel
git clone https://github.com/pyenv/pyenv.git $HOME/.pyenv

#try to find a way to change the .bashrc file to work with pyenv
# setup pyenv bash file
cp $(PYENV_BASHRC_PATH) $HOME/.bashrc #copy changes needed to make pyenv run to the .bashrc file
mkdir -p ~/pyenv_tmp #temp dir for pyenv installation of python versions

pyenv install $(PYTHON_VERSION)


#------ setting up airflow variables and connections, needs to be done in a airflowctl project dir

#--airflow env variables

#13140 hours is equivalent to 1,5 years
airflowctl airflow variables set --description "how far back will data be collected in hours, equivalent to 2,75 years" MAX_TIME_FRAME_HOURS  13140
airflowctl airflow variables set --description "how many hours are there to be between each daily update of the dataset " HOURS_BETWEEN_DAILY_UPDATES  12
airflowctl airflow variables set --description "ow many minutes of data does each row represent" MINS_PER_ROW  5

#--connections

# 1) export all connections to a file using airflowctl airflow connections export $(file)

# 2) Get the specific AWS connection (DO NOT SHARE IT ON GITHUB) and make a json file only with it (or keep the others ununsed connections)

# 3) Import the connections file using airflowctl airflow connections import $(file)