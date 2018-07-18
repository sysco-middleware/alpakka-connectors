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

    public static Source<ZMsg, NotUsed> subscribeClientSource(String addresses) {
        return Source.fromGraph(new ZmqSubscribeStage(false, addresses));
    }

    public static Source<ZMsg, NotUsed> subscribeServerSource(String addresses) {
        return Source.fromGraph(new ZmqSubscribeStage(true, addresses));
    }

    public static Source<ZMsg, NotUsed> pullServerSource(String addresses) {
        return Source.fromGraph(new ZmqPullStage(true, addresses));
    }

    public static Source<ZMsg, NotUsed> pullClientSource(String addresses) {
        return Source.fromGraph(new ZmqPullStage(false, addresses));
    }

    public static Sink<ZMsg, NotUsed> publishServerSink(String addresses) {
        return Flow.fromGraph(new ZmqPublishStage(true, addresses)).to(Sink.ignore());
    }

    public static Sink<ZMsg, NotUsed> publishClientSink(String addresses) {
        return Flow.fromGraph(new ZmqPublishStage(false, addresses)).to(Sink.ignore());
    }

    public static Flow<ZMsg, ZMsg, NotUsed> publishServerFlow(String addresses) {
        return Flow.fromGraph(new ZmqPublishStage(true, addresses));
    }

    public static Flow<ZMsg, ZMsg, NotUsed> publishClientFlow(String addresses) {
        return Flow.fromGraph(new ZmqPublishStage(false, addresses));
    }

    public static Sink<ZMsg, NotUsed> pushServerSink(String addresses) {
        return Flow.fromGraph(new ZmqPushStage(true, addresses)).to(Sink.ignore());
    }

    public static Sink<ZMsg, NotUsed> pushClientSink(String addresses) {
        return Flow.fromGraph(new ZmqPushStage(false, addresses)).to(Sink.ignore());
    }

    public static Flow<ZMsg, ZMsg, NotUsed> pushServerFlow(String addresses) {
        return Flow.fromGraph(new ZmqPushStage(true, addresses));
    }

    public static Flow<ZMsg, ZMsg, NotUsed> pushClientFlow(String addresses) {
        return Flow.fromGraph(new ZmqPushStage(false, addresses));
    }

    public static void main(String[] args) throws InterruptedException {
        ActorSystem system = ActorSystem.create("test");
        ActorMaterializer mat = ActorMaterializer.create(system);

//        Zmq.subscribeClientSource("tcp://localhost:8888")
//                .map(zmsg -> {
//                    System.out.println("middle: " + zmsg);
//                    return zmsg;
//                })
//                .to(Zmq.pushServerSink("tcp://*:8889"))
//                .run(mat);

        Zmq.subscribeClientSource("tcp://localhost:8881")
//        Zmq.pullClientSource("tcp://localhost:8889")
                .to(Sink.foreach(z -> System.out.println("end: " + z)))
                .run(mat);
        Thread.sleep(1000);
        Source.repeat("hello")
                .map(ZMsg::newStringMsg)
                .map(zmsg -> {
                    System.out.println("init: " + zmsg);
                    return zmsg;
                })
                .to(Zmq.publishServerSink("tcp://*:8881"))
                .run(mat);

    }
}
