#!/usr/bin/env bash

function qgen() {
  local BIN_DIR=$(dirname $0)
  local ROOT_DIR=$(dirname $BIN_DIR)
  local LIB_DIR=$ROOT_DIR/lib

  local CLASSPATH=
  for JAR in $(ls -1 $LIB_DIR/*.jar)
  do
    CLASSPATH=$CLASSPATH:$JAR
  done
  java -cp $CLASSPATH org.apache.asterixdb.querygenerator.QGen $*
}


rm -rf queries*
qgen $*
