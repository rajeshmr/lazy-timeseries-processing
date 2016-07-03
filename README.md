## Time series processing

A simple program to compute aggregates within a rolling time window. The input could be an infinite stream of data.

#### Compile and package 
```
sbt clean package
```

#### To Run
```
scala target/scala-2.11/lazy-timeseries-processing_2.11-1.0.jar src/main/resources/data_scala.txt
```

#### Test
```
sbt clean test
```


