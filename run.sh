#!/bin/sh
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH

java -classpath bin:"lib/*" edu.buffalo.cse562.Main "$@"
if [ $? -ne 0 ]; then
	echo "Make sure to run build.sh to build the project first!"
fi