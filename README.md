# Flink State Metadata Manipulation
## Usage
```
usage: java -jar state-metadata-1.0.0-SNAPSHOT.jar --input.file old_metadata --output.file _metadata \
  --input.uri s3://old-bucket/old-savepoints/savepoint-b9888f-a23df1784fa3 \
  --output.uri s3://new-bucket/new-savepoints/savepoint-b9888f-a23df1784fa3
```

## Requirements
Java and Maven

## Build
```
mvn clean package
```