#! /bin/sh
# help on apt-get: itsfoss.com/apt-get-linux-guide

# update the packages database for apt-get
sudo apt-get update

# upgrade the installed packages...add <package_name> for a specific package
sudo apt-get upgrade -y

# install a new package of name <package_name> with a specific version.  if no version will upgrade to latest
# or install latest
# sudo apt-get install <package_name>=<version_number>

# install python
sudo apt-get install python3.7

# install pip
sudo apt-get install -y python3-pip


# get the virtual env stuff
sudo apt-get install python3-venv
sudo apt-get install python3.7-venv

# install an editor if desired
# sudo apt-get nano
# sudo apt-get sublime


# create a virtual env called <env>
# python3 -m venv <env>

# activate venv
# source <env>/bin/activate


# now all pip installs will happen in venv
# upgrade to the latest version of pip (required for Tensorflow 2)
pip install --upgrade pip

# use pip freeze to find out packages
# use pip install requirements.txt to load all packages in file
pip install -r requirements.txt

# set username for git
# git config --global user.name "Jim Beaver"
# git config --global user.email "home@jimbeaver.com"

# set editor for git
# git config --global core.editor "<full path to editor>"

# download a particular branch .. will create a subdirectory called Bug-World
# git clone -branch <branch_name> https://github.com/BlestxVentures/Work_Spawner_3000.git

# update from repository
# git pull origin <branch_name>
