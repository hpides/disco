#!/usr/bin/env bash

# Use this by calling:
# bash -c "$(curl -sL https://raw.githubusercontent.com/lawben/distributed-scotty/benchmark/scripts/init.sh)"

BASE_DIR="/home/hadoop/benson"
BM_ENV_FILE="$BASE_DIR/benchmark_env"
BM_RUN_FILE="$BASE_DIR/run.sh"

mkdir -p "$BASE_DIR"
cd "$BASE_DIR"

if [ ! -d distributed-scotty ]; then
  git clone https://github.com/lawben/distributed-scotty.git
fi

cd distributed-scotty

# TODO: change this when needed
git checkout benchmark

#GRADLE_FILE="$BASE_DIR/gradle-build-output.txt"
#./gradlew build > "$GRADLE_FILE"
#
#CLASSPATH=$(cat "$GRADLE_FILE" | grep "^CLASSPATH: " | tail -n 1 | cut -c12-)
#export CLASSPATH=${CLASSPATH}
#echo "export CLASSPATH=$CLASSPATH" > "$BM_ENV_FILE"

# Clear run script
> "$BM_RUN_FILE"
chmod +x "$BM_RUN_FILE"

echo "cd $BASE_DIR" >> "$BM_RUN_FILE"
echo "kill -9 \$(cat /tmp/RUN_PID 2> /dev/null) &> /dev/null" >> "$BM_RUN_FILE"
echo "sleep 2" >> "$BM_RUN_FILE"
echo "sar -u 1 120 > "$BASE_DIR/cpu-util.log" &" >> "$BM_RUN_FILE"
echo "echo -e \"\nNEW RUN\n=======\"" >> "$BM_RUN_FILE"
echo "source benchmark_env" >> "$BM_RUN_FILE"
echo "echo BENCHMARK ARGS: \$BENCHMARK_ARGS" >> "$BM_RUN_FILE"
echo "echo \$\$ > /tmp/RUN_PID" >> "$BM_RUN_FILE"
echo "echo /usr/lib/jvm/java-11-openjdk-amd64/bin/java -cp /share/hadoop/benson/disco.jar com.github.lawben.disco.executables.\$BENCHMARK_ARGS" >> "$BM_RUN_FILE"
