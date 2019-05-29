#!/usr/bin/env bash

# Use this by calling:
# bash -c "$(curl -sL https://raw.githubusercontent.com/lawben/distributed-scotty/master/scripts/init.sh)"

sudo apt update
sudo apt install -y git openjdk-11-jdk

cd ~
HOME_DIR=${PWD}
git clone https://github.com/lawben/distributed-scotty.git
cd distributed-scotty

./gradlew build

CLASSPATH="\
$HOME_DIR/distributed-scotty:\
$HOME_DIR/distributed-scotty/out/production/classes:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/org.zeromq/jeromq/0.5.1/7ef8199a62e6bc91b549fcb49f85ccdf6ffc5078/jeromq-0.5.1.jar:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/com.github.lawben.scotty-window-processor/slicing/master-SNAPSHOT/72ff5f95901282921f469403e862c61c2bc47b95/slicing-master-SNAPSHOT.jar:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/com.github.lawben.scotty-window-processor/core/master-SNAPSHOT/8542a5c4eb28ce25ac1e4316ca00a59f04cf58c4/core-master-SNAPSHOT.jar:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/com.github.lawben.scotty-window-processor/state/master-SNAPSHOT/f2955c066fd8936af0b35c25ca74d3e6c4a7e735/state-master-SNAPSHOT.jar:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/eu.neilalexander/jnacl/1.0.0/82e9034fb81a33cb9d7e0c4cd241a2ba84802ae2/jnacl-1.0.0.jar:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.16/3a6274f658487d5bfff9af3862beff6da1e7fd52/slf4j-api-1.7.16.jar:\
$HOME_DIR/.gradle/caches/modules-2/files-2.1/org.jetbrains/annotations/17.0.0/8ceead41f4e71821919dbdb7a9847608f1a938cb/annotations-17.0.0.jar\
"

export CLASSPATH=${CLASSPATH}
echo "export CLASSPATH=$CLASSPATH" >> ${HOME_DIR}/.bashrc
