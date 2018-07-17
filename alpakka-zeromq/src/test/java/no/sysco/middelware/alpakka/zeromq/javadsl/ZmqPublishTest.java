package no.sysco.middelware.alpakka.zeromq.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.javadsl.TestKit;
import no.sysco.middleware.alpakka.zeromq.javadsl.Zmq;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class ZmqPublishTest {

    private ActorSystem system;
    private ActorMaterializer materializer;

    @Before
    public void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @Test
    public void shouldSendMessageWhenSourceElementAvailable() throws InterruptedException {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.SUB);
        socket.connect("tcp://localhost:5555");

        Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

        TestPublisher.Probe<ZMsg> probe = Zmq.publishSink("tcp://*:5555").runWith(testSource, materializer);

        probe.sendNext(ZMsg.newStringMsg("test"));
        probe.sendComplete();

        probe.ensureSubscription();


        socket.subscribe(ZMQ.SUBSCRIPTION_ALL);

        String s = socket.recvStr();

        Assert.assertEquals(s, "test");

        socket.close();
        context.term();
    }
}
