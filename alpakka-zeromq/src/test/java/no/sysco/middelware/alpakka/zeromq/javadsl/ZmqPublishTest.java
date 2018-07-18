package no.sysco.middelware.alpakka.zeromq.javadsl;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestPublisher;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import akka.stream.testkit.javadsl.TestSource;
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
    public void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @Test
    public void shouldPublishMessageWhenSourceElementAvailable() throws InterruptedException {
        //Given
        final ZMQ.Context context = ZMQ.context(1);
        final ZMQ.Socket socket = context.socket(ZMQ.SUB);
        socket.connect("tcp://localhost:5555");
        socket.subscribe(ZMQ.SUBSCRIPTION_ALL);

        Thread.sleep(200); //wait for subscriber to connect

        //When
        Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

        TestPublisher.Probe<ZMsg> probe = Zmq.publishServerSink("tcp://*:5555").runWith(testSource, materializer);

        probe.ensureSubscription();
        Thread.sleep(200); //wait for published to bind

        //Then
        probe.sendNext(ZMsg.newStringMsg("test"));

        String s = socket.recvStr();
        Assert.assertEquals(s, "test");

        socket.close();
        context.term();
    }

    @Test
    public void shouldEmitElementWhenSubscriptionReceiveMessage() throws InterruptedException {
        //Given
        final ZMQ.Context context = ZMQ.context(1);
        final ZMQ.Socket socket = context.socket(ZMQ.PUB);
        socket.bind("tcp://*:5556");

        Thread.sleep(200); //wait for subscriber to connect

        //When
        Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
        TestSubscriber.Probe<ZMsg> probe = Zmq.subscribeClientSource("tcp://localhost:5556").runWith(testSink, materializer);

        probe.ensureSubscription();
        Thread.sleep(200); //wait for published to bind

        //Then
        socket.send("test");
        Thread.sleep(200); //wait for published to bind

        ZMsg zMsg = probe.requestNext();
        Assert.assertEquals("test", zMsg.popString());

        socket.close();
        context.term();
    }
}
