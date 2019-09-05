#!/usr/bin/env bash

# Use this by calling:
# bash -c "$(curl -sL https://raw.githubusercontent.com/lawben/distributed-scotty/master/scripts/init.sh)"

sudo apt update
sudo apt install -y git htop sysstat openjdk-11-jdk ntp

cd ~
git clone https://github.com/lawben/distributed-scotty.git
cd distributed-scotty

# TODO: change this when needed
git checkout benchmark

./gradlew build > ~/gradle-build-output.txt

CLASSPATH=$(cat ~/gradle-build-output.txt | grep "^CLASSPATH: " | tail -n 1 | cut -c12-)
export CLASSPATH=${CLASSPATH}
echo "export CLASSPATH=$CLASSPATH" >> ~/benchmark_env

service ntp reload

echo "pkill -9 java" >> ~/run.sh
echo "sleep 3" >> ~/run.sh
echo "sar -u 1 120 > util.log &" >> ~/run.sh
echo "echo -e \"\nNEW RUN\n=======\"" >> ~/run.sh
echo "source benchmark_env" >> ~/run.sh
echo "echo BENCHMARK ARGS: \$BENCHMARK_ARGS" >> ~/run.sh
echo "echo \$\$ > /tmp/RUN_PID" >> ~/run.sh

chmod +x ~/run.sh
