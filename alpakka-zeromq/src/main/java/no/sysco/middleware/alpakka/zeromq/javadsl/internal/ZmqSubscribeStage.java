package no.sysco.middleware.alpakka.zeromq.javadsl.internal;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.CompletableFuture;

public class ZmqSubscribeStage extends GraphStage<SourceShape<ZMsg>> {

  private final boolean isServer;
  private final String addresses;
  private final String subscription;

  private final Outlet<ZMsg> outlet = Outlet.create("ZmqSubscribe.out");
  private final SourceShape<ZMsg> shape = new SourceShape<>(outlet);

  public ZmqSubscribeStage(boolean isServer, String addresses) {
    this.isServer = isServer;
    this.addresses = addresses;
    this.subscription = null;
  }

  public ZmqSubscribeStage(boolean isServer, String addresses, String subscription) {
    this.isServer = isServer;
    this.addresses = addresses;
    this.subscription = subscription;
  }

  @Override
  public SourceShape<ZMsg> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    if (isServer) {
      throw new UnsupportedOperationException("Server SUB Socket type is not supported yet.");
    }

    return new ZmqStageLogic.ClientStageLogic(shape, addresses, ZMQ.SUB) {
      @Override
      public void preStart() throws Exception {
        super.preStart();
        if (subscription == null) {
          socket().subscribe(ZMQ.SUBSCRIPTION_ALL);
        } else {
          socket().subscribe(subscription);
        }
      }

      {
        setHandler(shape.out(), new AbstractOutHandler() {
          @Override
          public void onPull() throws Exception {
            final ZMsg elem = ZMsg.recvMsg(socket(), true);
            if (elem != null) {
              push(shape.out(), elem);
            }
          }
        });
      }
    };
  }
}
