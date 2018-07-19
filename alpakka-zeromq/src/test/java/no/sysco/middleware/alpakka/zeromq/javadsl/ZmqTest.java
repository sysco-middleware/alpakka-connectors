package no.sysco.middleware.alpakka.zeromq.javadsl;

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

import java.io.IOException;
import java.net.ServerSocket;

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
    final int port = findFreePort();
    final String addr = "tcp://localhost:" + port;
   //When
    Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

    TestPublisher.Probe<ZMsg> probe = Zmq.publishServerSink(addr).runWith(testSource, materializer);

    probe.ensureSubscription();
    Thread.sleep(200); //wait for published to bind

    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.SUB);
    socket.connect(addr);
    socket.subscribe(ZMQ.SUBSCRIPTION_ALL);

    Thread.sleep(200); //wait for subscriber to connect


    //Then
    probe.sendNext(ZMsg.newStringMsg("test"));
    Thread.sleep(200);

    String s = socket.recvStr(ZMQ.DONTWAIT);
    Assert.assertEquals(s, "test");

    socket.close();
    context.term();
  }

  @Test
  public void shouldServerPushMessageWhenSourceElementAvailable() throws InterruptedException {
    //Given
    final int port = findFreePort();
    final String addr = "tcp://localhost:" + port;
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PULL);
    socket.connect(addr);

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

    TestPublisher.Probe<ZMsg> probe = Zmq.pushServerSink(addr).runWith(testSource, materializer);

    probe.ensureSubscription();
    Thread.sleep(200); //wait for published to bind

    //Then
    probe.sendNext(ZMsg.newStringMsg("test"));
    Thread.sleep(200); //wait for published to bind

    String s = socket.recvStr(ZMQ.DONTWAIT);
    Assert.assertEquals(s, "test");

    socket.close();
    context.term();
  }

  @Test
  public void shouldClientPushMessageWhenSourceElementAvailable() throws InterruptedException {
    //Given
    final int port = findFreePort();
    final String addr = "tcp://localhost:" + port;
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PULL);
    socket.bind(addr);

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Source<ZMsg, TestPublisher.Probe<ZMsg>> testSource = TestSource.probe(system);

    TestPublisher.Probe<ZMsg> probe = Zmq.pushClientSink(addr).runWith(testSource, materializer);

    probe.ensureSubscription();
    Thread.sleep(200); //wait for published to bind

    //Then
    probe.sendNext(ZMsg.newStringMsg("test"));
    Thread.sleep(200); //wait for published to bind

    String s = socket.recvStr(ZMQ.DONTWAIT);
    Assert.assertEquals(s, "test");

    socket.close();
    context.term();
  }

  @Test
  public void shouldEmitElementWhenClientSubscriptionReceiveMessage() throws InterruptedException {
    //Given
    final int port = findFreePort();
    final String addr = "tcp://localhost:" + port;
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PUB);
    socket.bind(addr);

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
    TestSubscriber.Probe<ZMsg> probe = Zmq.subscribeClientSource(addr).runWith(testSink, materializer);

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
    final int port = findFreePort();
    final String addr = "tcp://localhost:" + port;
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PUSH);
    socket.bind(addr);

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
    TestSubscriber.Probe<ZMsg> probe = Zmq.pullClientSource(addr).runWith(testSink, materializer);

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
    final int port = findFreePort();
    final String addr = "tcp://localhost:" + port;
    final ZMQ.Context context = ZMQ.context(1);
    final ZMQ.Socket socket = context.socket(ZMQ.PUSH);
    socket.connect(addr);

    Thread.sleep(200); //wait for subscriber to connect

    //When
    Sink<ZMsg, TestSubscriber.Probe<ZMsg>> testSink = TestSink.probe(system);
    TestSubscriber.Probe<ZMsg> probe = Zmq.pullServerSource(addr).runWith(testSink, materializer);

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

  private static int findFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      try {
        socket.close();
      } catch (IOException e) {
        // Ignore IOException on close()
      }
      return port;
    } catch (IOException ignored) {
    }
    throw new IllegalStateException("Could not find a free TCP/IP port to run test");
  }
}
