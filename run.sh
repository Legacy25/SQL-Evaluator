#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH
java -classpath bin:"lib/*" edu.buffalo.cse562.Main "$@"
