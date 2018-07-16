package no.sysco.middleware.alpakka.zeromq.javadsl.internal;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.GraphStageLogicWithLogging;
import akka.util.ByteString;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Set;

public class ZeroMQPullSourceStage extends GraphStage<SourceShape<ByteString>> {

    private final Set<String> addresses;

    private final Outlet<ByteString> outlet = Outlet.create("ZeroMQ.out");
    private final SourceShape<ByteString> shape = new SourceShape<>(outlet);

    public ZeroMQPullSourceStage(Set<String> addresses) {
        this.addresses = addresses;
    }

    @Override
    public SourceShape<ByteString> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return new GraphStageLogic(shape) {

            private ZContext zContext;
            private ZMQ.Socket socket;

            @Override
            public void preStart() throws Exception {
                super.preStart();
                zContext = new ZContext();
                socket = zContext.createSocket(ZMQ.PULL);
                String join = String.join(",", addresses);
                socket.connect(join);
            }

            @Override
            public void postStop() throws Exception {
                zContext.destroySocket(socket);
                zContext.close();
                super.postStop();
            }

            {
                setHandler(outlet, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        try {
                            byte[] recv = socket.recv();
                            push(outlet, ByteString.fromArray(recv));
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                });
            }
        };
    }
}
