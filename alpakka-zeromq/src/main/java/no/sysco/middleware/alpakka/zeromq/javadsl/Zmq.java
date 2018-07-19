package no.sysco.middleware.alpakka.zeromq.javadsl;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqPublishStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqPullStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqPushStage;
import no.sysco.middleware.alpakka.zeromq.javadsl.internal.ZmqSubscribeStage;
import org.zeromq.ZMsg;

public class Zmq {

  /**
   * Creates a Source for a Subscribe socket type client.
   *
   * @param addresses Socket address to connect.
   * @return A Source of {@link ZMsg} elements coming from a `PUB` server.
   */
  public static Source<ZMsg, NotUsed> subscribeClientSource(String addresses) {
    return Source.fromGraph(new ZmqSubscribeStage(false, addresses));
  }

  /**
   * Creates a Source for a Subscribe socket type client.
   *
   * @param addresses Socket address to connect.
   * @param subscription String to subscribe to.
   * @return A Source of {@link ZMsg} elements coming from a `PUB` server.
   */
  public static Source<ZMsg, NotUsed> subscribeClientSource(String addresses, String subscription) {
    return Source.fromGraph(new ZmqSubscribeStage(false, addresses, subscription));
  }

  /**
   * Creates a Source for a Pull socket type server.
   *
   * @param addresses Socket address to bind.
   * @return A Source of {@link ZMsg} elements coming from a `PUSH` client.
   */
  public static Source<ZMsg, NotUsed> pullServerSource(String addresses) {
    return Source.fromGraph(new ZmqPullStage(true, addresses));
  }

  /**
   * Creates a Source for a Pull socket type client.
   *
   * @param addresses Socket address to connect.
   * @return A Source of {@link ZMsg} elements coming from a `PUSH` server.
   */
  public static Source<ZMsg, NotUsed> pullClientSource(String addresses) {
    return Source.fromGraph(new ZmqPullStage(false, addresses));
  }

  /**
   * Creates a Sink for a Publish socket type server.
   *
   * @param addresses Socket address to bind.
   * @return A Sink of {@link ZMsg} elements coming to a `SUB` client.
   */
  public static Sink<ZMsg, NotUsed> publishServerSink(String addresses) {
    return Flow.fromGraph(new ZmqPublishStage(true, addresses)).to(Sink.ignore());
  }

  /**
   * Creates a Flow for a Publish socket type server.
   *
   * @param addresses Socket address to bind.
   * @return A Flow of {@link ZMsg} elements coming to a `SUB` client.
   */
  public static Flow<ZMsg, ZMsg, NotUsed> publishServerFlow(String addresses) {
    return Flow.fromGraph(new ZmqPublishStage(true, addresses));
  }

  /**
   * Creates a Sink for a Push socket type server.
   *
   * @param addresses Socket address to bind.
   * @return A Sink of {@link ZMsg} elements coming to a `PULL` client.
   */
  public static Sink<ZMsg, NotUsed> pushServerSink(String addresses) {
    return Flow.fromGraph(new ZmqPushStage(true, addresses)).to(Sink.ignore());
  }

  /**
   * Creates a Sink for a Push socket type client.
   *
   * @param addresses Socket address to connect.
   * @return A Sink of {@link ZMsg} elements coming to a `PULL` server.
   */
  public static Sink<ZMsg, NotUsed> pushClientSink(String addresses) {
    return Flow.fromGraph(new ZmqPushStage(false, addresses)).to(Sink.ignore());
  }

  /**
   * Creates a Flow for a Push socket type server.
   *
   * @param addresses Socket address to bind.
   * @return A Flow of {@link ZMsg} elements coming to a `PULL` client.
   */
  public static Flow<ZMsg, ZMsg, NotUsed> pushServerFlow(String addresses) {
    return Flow.fromGraph(new ZmqPushStage(true, addresses));
  }

  /**
   * Creates a Flow for a Push socket type client.
   *
   * @param addresses Socket address to connect.
   * @return A Flow of {@link ZMsg} elements coming to a `PULL` server.
   */
  public static Flow<ZMsg, ZMsg, NotUsed> pushClientFlow(String addresses) {
    return Flow.fromGraph(new ZmqPushStage(false, addresses));
  }
}
