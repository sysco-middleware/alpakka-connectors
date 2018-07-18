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
        //Given
        final ZMQ.Context context = ZMQ.context(1);
        final ZMQ.Socket socket = context.socket(ZMQ.SUB);
        socket.connect("tcp://localhost:5555");
        socket.subscribe(ZMQ.SUBSCRIPTION_ALL);

        Thread.sleep(1000); //wait for subscriber to connect

        //When
        Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

        TestPublisher.Probe<ZMsg> probe = Zmq.publishServerSink("tcp://*:5555").runWith(testSource, materializer);

        probe.ensureSubscription();
        Thread.sleep(1000); //wait for published to bind

        //Then
        probe.sendNext(ZMsg.newStringMsg("test"));

        String s = socket.recvStr();
        Assert.assertEquals(s, "test");

        socket.close();
        context.term();
    }
}
