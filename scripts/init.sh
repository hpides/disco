#!/usr/bin/env bash

# Use this by calling:
# bash -c "$(curl -sL https://raw.githubusercontent.com/lawben/distributed-scotty/master/scripts/init.sh)"

sudo apt update
sudo apt install -y git openjdk-11-jdk

cd ~
HOME_DIR=${PWD}
git clone https://github.com/lawben/distributed-scotty.git
cd distributed-scotty

./gradlew build > gradle-build-output.txt

CLASSPATH=$(cat gradle-build-output.txt | grep "^CLASSPATH: " | cut -c12-)
export CLASSPATH=${CLASSPATH}
echo "export CLASSPATH=$CLASSPATH" >> ${HOME_DIR}/.bashrc
