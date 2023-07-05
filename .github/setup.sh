#!/bin/bash
set -ex

###########################################################
# JAVA
###########################################################

mkdir -p /opt/openjdk
pushd /opt/openjdk > /dev/null
JDK_URL="https://github.com/AdoptOpenJDK/openjdk9-binaries/releases/download/jdk-9.0.4%2B11/OpenJDK9U-jdk_x64_linux_hotspot_9.0.4_11.tar.gz"
mkdir java9
pushd java9 > /dev/null
curl -L ${JDK_URL} --output OpenJDK9U-jdk_x64_linux_hotspot_9.0.4_11.tar.gz