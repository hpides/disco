#!/usr/bin/env bash
set -e

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="$(cd $FILE_DIR/.. && pwd)"
cd "$BASE_DIR"

./gradlew shadowJar > /dev/null

scp benchmarks/build/libs/benchmarks-1.0-SNAPSHOT-all.jar benson@sshgate:disco.jar
ssh benson@sshgate "scp disco.jar hadoop@cloud-11.dima.tu-berlin.de:/share/hadoop/benson/disco.jar"
