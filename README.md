# triangle-demo
This project uses Apache Spark to find the most common words in a text file

## To build a fat jar
```
mvn clean compile assembly:single
```

## To execute
```
java -cp target/spark-top-words-1.0-SNAPSHOT-jar-with-dependencies.jar com.marzeta.words.TopWords <FILE_URL> [MAX_RESULTS]

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
