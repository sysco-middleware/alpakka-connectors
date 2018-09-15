# Alpakka Connectors

[![Build Status](https://travis-ci.org/sysco-middleware/alpakka-connectors.svg?branch=master)](https://travis-ci.org/sysco-middleware/alpakka-connectors)


[![Maven metadata URI](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/no/sysco/middleware/alpakka/alpakka-connectors/maven-metadata.xml.svg)](https://repo1.maven.org/maven2/no/sysco/middleware/alpakka/alpakka-connectors)

## File Connectors

### Listening to changes in a directory

The RecursiveDirectoryChangesSource tries to improve the existing DirectoryChangesSource from alpakka with recursive 
folder monitoring. It will emit elements every time there is a change to a watched directory in the local file system 
or any of the subdirectories (new or existing). The enumeration consists of the path that was changed and an 
enumeration describing what kind of change it was.

```java
import no.sysco.middleware.alpakka.files.javadsl.RecursiveDirectoryChangesSource;

public class App {
    public static void main(String[] args){
        final ActorSystem system = ActorSyste.create();
        final ActorMaterializer mat = ActorMaterializer.create(system);
        
        final FileSystem fs = FileSystems.getDefault();
        final Duration pollingInterval = Duration.of(1, ChronoUnit.SECONDS);
        final int maxBufferSize = 1000;
        final Source<Pair<Path, DirectoryChange>, NotUsed> changes =
            RecursiveDirectoryChangesSource.create(fs.getPath(path), pollingInterval, maxBufferSize);


        changes.runForeach((Pair<Path, DirectoryChange> pair) -> {
            final Path changedPath = pair.first();
            final DirectoryChange change = pair.second();
            System.out.println("Path: " + changedPath + ", Change: " + change);
        }, mat);
    }
}
```

## ZeroMQ Connectors

ZeroMQ Connector uses JeroMQ library to expose Source, Flow and Sinks based on ZeroMQ Socket types 
(e.g. `PUB/SUB`, `PULL/PUSH`).

```java
import no.sysco.middleware.alpakka.zeromq.javadsl.Zmq;

public class App {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create();
        final ActorMaterializer mat = ActorMaterializer.create(system);
        
        Source.repeat("hello")
              .map(ZMsg::createNewMsg)
              .to(Zmq.publishServerSink("tcp://*:5555"))
              .run(mat);
        
        Zmq.subscribeClientSource("tcp://localhost:5555")
           .map(zmsg -> {
               System.out.println(zmsg.popString());
               return zmsg;
           })
           .runWith(Sink.ignore(), mat);
    }
}
```

## Zipkin-Brave Connectors

Brave connectors create Spans flows to integrate tracing into your Akka Streams applications.

```java
public class App {
  public static void main(String[] args){
    final ActorSystem system = ActorSystem.create();
        final ActorMaterializer mat = ActorMaterializer.create(system);
        
        final Tracing tracing = ...;
        
        Source.repeat("hello")
          .via(Brave.startSpanFlow(tracing, "akka-stream-trace"))
          .via(Brave.childSpanFlow(tracing, "akka-stream-span-map", Flow.<String>create().map(s -> s)))
          .via(Brave.finishSpanFlow(tracing))
          .to(Sink.ignore())
          .run(materializer);
  }
}
```