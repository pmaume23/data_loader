# Data Loader Project

## Overview
The Data Loader project is a Scala-based application that utilizes Apache Spark to 
process and analyze stock transaction data stored in Parquet files. The project includes 
several helper classes to manage Spark sessions, read Parquet files, and generate reports 
on data completeness.

## Project Structure
The project is organized as follows:
- `src/main/scala/com/physie/Main.scala`: The entry point of the application
- `src/main/scala/com/physie/Helpers/SparkParquetHelper.scala`: Helper object to manage 
Spark sessions and read Parquet files
- `src/main/scala/com/physie/Helpers/DataFrameHelper.scala`: Helper object to generate 
reports on data completeness
- `src/main/scala/com/physie/nodes/CompletenessNode.scala`: Class to perform completeness checks 
on stock transaction data
- `pom.xml`: Maven project configuration file
- `scripts/run_main.sh`: Shell script to run the application


## Pre-requisites
- Java 8 or higher
- Scala 2.12.15
- Apache Spark 3.3.2
- Maven

## Building the Project
To build the project, navigate to the project directory and run the following command:
```mvn clean install```
This will compile the project and generate a JAR file in the `target` directory.

## Running the Application
To run the application, navigate to the project directory and run the following command:
```./scripts/run_main.sh "<path_to_jar_file>" "package_name_of_main_class"```

