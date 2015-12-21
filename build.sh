#!/bin/bash
set -e

java -version
./gradlew clean shadowJar
