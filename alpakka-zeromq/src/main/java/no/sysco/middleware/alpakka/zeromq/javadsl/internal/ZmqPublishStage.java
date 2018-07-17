package no.sysco.middleware.alpakka.zeromq.javadsl.internal;

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

public class ZmqPublishStage extends GraphStage<FlowShape<ZMsg, ZMsg>> {

    private final String addresses;

    private final Inlet<ZMsg> inlet = Inlet.create("ZeroMQPublish.in");
    private final Outlet<ZMsg> outlet = Outlet.create("ZeroMQPublish.out");
    private final FlowShape<ZMsg, ZMsg> shape = new FlowShape<>(inlet, outlet);

    public ZmqPublishStage(String addresses) {
        this.addresses = addresses;
    }

    @Override
    public FlowShape<ZMsg, ZMsg> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return new ZmqStageLogic.ServerStageLogic(shape, addresses, ZMQ.PUB) {
            {
                setHandler(shape.in(), new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        final ZMsg elem = grab(shape.in());
                        elem.send(socket());
                        push(shape.out(), elem);
                    }
                });

                setHandler(shape.out(), new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                       tryPull(shape.in());
                    }
                });
            }
        };
    }
}
