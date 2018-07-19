package no.sysco.middleware.alpakka.zeromq.javadsl.internal;

import akka.annotation.ApiMayChange;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

/**
 * Experimental
 */
@ApiMayChange
public class ZmqRequestStage extends GraphStage<FlowShape<ZMsg, ZMsg>> {

  private final String addresses;

  private final Inlet<ZMsg> inlet = Inlet.create("ZmqPublish.in");
  private final Outlet<ZMsg> outlet = Outlet.create("ZmqPublish.out");
  private final FlowShape<ZMsg, ZMsg> shape = new FlowShape<>(inlet, outlet);

  public ZmqRequestStage(String addresses) {
    this.addresses = addresses;
  }

  @Override
  public FlowShape<ZMsg, ZMsg> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return new ZmqStageLogic.ClientStageLogic(shape, addresses, ZMQ.REQ) {
      {
        setHandler(shape.in(), new AbstractInHandler() {
          @Override
          public void onPush() throws Exception {
            final ZMsg elem = grab(shape.in());
            elem.send(socket());
            final ZMsg reply = ZMsg.recvMsg(socket(), false);
            push(shape.out(), reply);
          }
        });

        setHandler(shape.out(), new AbstractOutHandler() {
          @Override
          public void onPull() throws Exception {
            tryPull(shape().in());
          }
        });
      }
    };
  }
}
