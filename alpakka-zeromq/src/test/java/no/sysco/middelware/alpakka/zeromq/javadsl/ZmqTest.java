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

public class ZmqTest {

  private ActorSystem system;
  private ActorMaterializer materializer;

  @Before
  public void setup() {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
  }

  @Test
  public void shouldServerPublishMessageWhenSourceElementAvailable() throws InterruptedException {
    //Given
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.SUB);
    socket.connect("tcp://localhost:5561");
    socket.subscribe(ZMQ.SUBSCRIPTION_ALL);

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

    TestPublisher.Probe<ZMsg> probe = Zmq.publishServerSink("tcp://*:5561").runWith(testSource, materializer);

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
  public void shouldServerPushMessageWhenSourceElementAvailable() throws InterruptedException {
    //Given
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PULL);
    socket.connect("tcp://localhost:5555");

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

    TestPublisher.Probe<ZMsg> probe = Zmq.pushServerSink("tcp://*:5555").runWith(testSource, materializer);

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
  public void shouldClientPushMessageWhenSourceElementAvailable() throws InterruptedException {
    //Given
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PULL);
    socket.bind("tcp://*:5557");

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

    TestPublisher.Probe<ZMsg> probe = Zmq.pushClientSink("tcp://localhost:5557").runWith(testSource, materializer);

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
  public void shouldEmitElementWhenClientSubscriptionReceiveMessage() throws InterruptedException {
    //Given
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PUB);
    socket.bind("tcp://*:5559");

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
    TestSubscriber.Probe<ZMsg> probe = Zmq.subscribeClientSource("tcp://localhost:5559").runWith(testSink, materializer);

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

  @Test
  public void shouldEmitElementWhenClientPullReceiveMessage() throws InterruptedException {
    //Given
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PUSH);
    socket.bind("tcp://*:5558");

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
    TestSubscriber.Probe<ZMsg> probe = Zmq.pullClientSource("tcp://localhost:5558").runWith(testSink, materializer);

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

    @Test
  public void shouldEmitElementWhenServerPullReceiveMessage() throws InterruptedException {
    //Given
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PUSH);
    socket.connect("tcp://localhost:5560");

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
    TestSubscriber.Probe<ZMsg> probe = Zmq.pullServerSource("tcp://localhost:5560").runWith(testSink, materializer);

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
