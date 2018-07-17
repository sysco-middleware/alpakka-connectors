package no.sysco.middleware.alpakka.zeromq.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqPublishStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqPullStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqPushStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqSubscribeStage;
import org.zeromq.ZMsg;

public class Zmq {

    public static Source<ZMsg, NotUsed> subscribeSource(String addresses) {
        return Source.fromGraph(new ZmqSubscribeStage(addresses));
    }

    public static Source<ZMsg, NotUsed> pullServerSource(String addresses) {
        return Source.fromGraph(new ZmqPullStage(true, addresses));
    }

    public static Source<ZMsg, NotUsed> pullClientSource(String addresses) {
        return Source.fromGraph(new ZmqPullStage(false, addresses));
    }

    public static Sink<ZMsg, NotUsed> publishSink(String addresses) {
        return Flow.fromGraph(new ZmqPublishStage(addresses)).to(Sink.ignore());
    }

    public static Sink<ZMsg, NotUsed> pushServerSink(String addresses) {
        return Flow.fromGraph(new ZmqPushStage(true, addresses)).to(Sink.ignore());
    }

    public static Sink<ZMsg, NotUsed> pushClientSink(String addresses) {
        return Flow.fromGraph(new ZmqPushStage(false, addresses)).to(Sink.ignore());
    }

    public static void main(String[] args) throws InterruptedException {
        ActorSystem system = ActorSystem.create("test");
        ActorMaterializer mat = ActorMaterializer.create(system);

        Zmq.subscribeSource("tcp://localhost:5555")
                .map(zmsg -> {
                    System.out.println("middle: " + zmsg);
                    return zmsg;
                })
                .to(Zmq.pushServerSink("tcp://*:5556"))
                .run(mat);

        Source.repeat("hello")
                .map(ZMsg::newStringMsg)
                .map(zmsg -> {
                    System.out.println("init: " + zmsg);
                    return zmsg;
                })
                .to(Zmq.publishSink("tcp://*:5555"))
                .run(mat);

        Zmq.pullClientSource("tcp://localhost:5556")
                .to(Sink.foreach(z -> System.out.println("end: " + z)))
                .run(mat);
    }
}
