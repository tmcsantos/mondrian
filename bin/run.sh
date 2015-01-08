#!/bin/bash
# do not use ksh, use bash
#####################################################################
#
# runcmd.sh
#
# Must set location of the cmdrunner.jar
#
# $Id$
#####################################################################

declare -r DIR=$(dirname $0)

LOCATION="$PWD/$DIR"
MONDRIAN_HOME="$LOCATION/.."
MONDRIAN_LIB="$MONDRIAN_HOME/lib"
MONDRIAN_TEST_LIB="$MONDRIAN_HOME/test-lib"

for j in $MONDRIAN_LIB/*.jar
do
    CLASSPATH="$CLASSPATH:$j"
done

# now pick up jdbc jars
for j in $MONDRIAN_TEST_LIB/*.jar
do  
    CLASSPATH="$CLASSPATH:$j"
done

export CLASSPATH

MAIN=mondrian.tui.CmdRunner

java \
    -Dlog4j.configuration=file://$MONDRIAN_HOME/log4j.properties \
    -cp $CLASSPATH $MAIN "$@"

