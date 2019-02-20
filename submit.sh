#!/bin/bash
#
# Copyright 2019 Institut Laue–Langevin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#



MASTER_CLUSTER=spark://puma-spark-1.ill.fr:7077
MASTER_LOCAL="local[*]"
DRIVER_JAVA_OPTIONS=" "

UNKNOWN="unknown"

usage() {
    echo "submit.sh <APP> [--master|-m (local, cluster)] [--env|-e (prod,dev)] [--no-build|-nb]"
}

classForApp() {
    case "$1" in
        matcher) echo "eu.ill.puma.sparkmatcher.matching.app.FullMatcherApp";;
        testmatcher) echo "eu.ill.puma.sparkmatcher.matching.app.TestMatcherApp";;
        optimizer) echo "eu.ill.puma.sparkmatcher.matching.app.OptimizerApp";;
        test) echo "eu.ill.puma.sparkmatcher.matching.app.TestApp";;
        documentDeduplicator) echo "eu.ill.puma.sparkmatcher.deduplication.dedup.DocumentDeduplicatorApp";;
        personDeduplicator) echo "eu.ill.puma.sparkmatcher.deduplication.dedup.PersonDeduplicatorApp";;
        laboratoryDeduplicator) echo "eu.ill.puma.sparkmatcher.deduplication.dedup.LaboratoryDeduplicatorApp";;
        evaluator) echo "eu.ill.puma.sparkmatcher.matching.app.RankEvaluatorApp";;
        testML) echo "eu.ill.puma.sparkmatcher.test.TestMl";;
        *) echo "$UNKNOWN";;
    esac

}

masterForOption() {
    case "$1" in
        local) echo "$MASTER_LOCAL";;
        cluster) echo "$MASTER_CLUSTER";;
        *) echo "$UNKNOWN";;
    esac
}

if [ "$#" -lt 1 ]; then
    usage
    exit 1
fi

# Get runner class
APP="$1"
RUNNER_CLASS=$(classForApp "$APP")

if [ "$RUNNER_CLASS" = "$UNKNOWN" ]; then
    echo "unknown App \"$APP\""
    exit 1
fi

#Default master on local
MASTER="$MASTER_LOCAL"
BUILD=1
ENVIRONMENT=dev

# Parse options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -m|--master)
            MASTER=$(masterForOption "$2")

            if [ "$MASTER" = "$UNKNOWN" ]; then
                echo "unknown master \"$2\""
                exit 1
            fi

            shift # past argument
            shift # past value
        ;;
        -e|--env)
            ENVIRONMENT=$2
            DRIVER_JAVA_OPTIONS="$DRIVER_JAVA_OPTIONS -Dpuma.matching.environment=$ENVIRONMENT"

            shift # past argument
            shift # past value
        ;;
        -nb|--no-build)
            BUILD=0

            shift # past argument
        ;;
        *)    # unknown option
            shift # past argument
        ;;
    esac
done

if [ "$BUILD" -eq 1 ]; then
    echo "Building application"

    mvn clean
    mvn package
fi


echo "=======> Running Puma Spark Application\n  App = \"$APP\"\n  class = \"$RUNNER_CLASS\"\n  master = \"$MASTER\"\n  env = \"$ENVIRONMENT\""

mkdir -p /tmp/spark-events

"$SPARK_HOME"/bin/spark-submit --driver-memory 22g --conf "spark.driver.extraJavaOptions=$DRIVER_JAVA_OPTIONS" --master "$MASTER" --class "$RUNNER_CLASS" ./target/spark-matcher-0.3.jar