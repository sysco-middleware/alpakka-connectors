package no.sysco.middleware.alpakka.zeromq.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZeroMQPullSourceStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZeroMQSubSourceStage;

import java.util.Collections;
import java.util.Set;

public class ZeroMQ {

    static Source<ByteString, NotUsed> subSource(Set<String> addresses) {
        return Source.fromGraph(new ZeroMQSubSourceStage(addresses));
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("test");
        ActorMaterializer mat = ActorMaterializer.create(system);

        ZeroMQ.subSource(Collections.singleton("tcp://localhost:5563"))
                .runWith(Sink.foreach(System.out::println), mat);
    }
}
