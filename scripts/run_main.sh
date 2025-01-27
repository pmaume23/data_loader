#!/bin/bash

#Check if the JAR file path is provided
if [ -z "$1" ]
  then
    echo "Please provide the path to the JAR file"
    exit 1
fi

#Check if the main class name is provided
if [ -z "$2" ]
  then
    echo "Please provide the main class name"
    exit 1
fi

#Set the path to the JAR file
JAR_FILE=$1

#Set the main class name
MAIN_CLASS=$2

#Run the application
spark-submit --class $MAIN_CLASS --master local $JAR_FILE