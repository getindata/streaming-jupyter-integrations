#!/usr/bin/env bash

FLINK_VERSION=${FLINK_VERSION:-1.15.1}
FLINK_TABLE_PLANNER_LOADER_JAR=flink-table-planner-loader-${FLINK_VERSION}.jar

mvn clean compile -Dflink.version="$FLINK_VERSION"

mkdir work
cd work || exit

wget "https://repo1.maven.org/maven2/org/apache/flink/flink-table-planner-loader/${FLINK_VERSION}/${FLINK_TABLE_PLANNER_LOADER_JAR}"
jar xvf "${FLINK_TABLE_PLANNER_LOADER_JAR}"
rm "${FLINK_TABLE_PLANNER_LOADER_JAR}"

jar -uf flink-table-planner.jar -C ../target/classes/ 'org/apache/flink/table/planner/plan/metadata/FlinkRelMetadataQuery.class'
jar -uf flink-table-planner.jar -C ../target/classes/ 'org/apache/flink/table/planner/plan/metadata/FlinkRelMetadataQuery$1.class'
jar -uf flink-table-planner.jar -C ../target/classes/ 'org/apache/flink/table/planner/plan/metadata/FlinkRelMetadataQuery$Handlers.class'

jar cvfM "$FLINK_TABLE_PLANNER_LOADER_JAR" flink-table-planner.jar META-INF org
