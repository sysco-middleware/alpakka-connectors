package no.sysco.middleware.alpakka.zeromq.javadsl.internal;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class ZmqPullStage extends GraphStage<SourceShape<ZMsg>> {

  private final boolean isServer;
  private final String addresses;

  private final Outlet<ZMsg> outlet = Outlet.create("ZmqPull.out");
  private final SourceShape<ZMsg> shape = new SourceShape<>(outlet);

  public ZmqPullStage(boolean isServer, String addresses) {
    this.isServer = isServer;
    this.addresses = addresses;
  }

  @Override
  public SourceShape<ZMsg> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return isServer ?
        new ZmqStageLogic.ServerStageLogic(shape, addresses, ZMQ.PULL) {
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
        } :
        new ZmqStageLogic.ClientStageLogic(shape, addresses, ZMQ.PULL) {
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
