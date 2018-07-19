package no.sysco.middleware.alpakka.zeromq.javadsl.internal;

import akka.stream.Shape;
import akka.stream.stage.GraphStageLogic;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public abstract class ZmqStageLogic extends GraphStageLogic {

  private final ZContext zContext;
  private final ZMQ.Socket socket;
  private final String addresses;

  ZmqStageLogic(Shape shape,
                String addresses,
                int socketType) {
    super(shape);
    this.addresses = addresses;
    this.zContext = new ZContext();
    this.socket = zContext.createSocket(socketType);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    zContext.destroy();
    super.postStop();
  }

  ZMQ.Socket socket() {
    return socket;
  }

  String getAddresses() {
    return addresses;
  }

  public static class ClientStageLogic extends ZmqStageLogic {
    ClientStageLogic(Shape shape,
                     String addresses,
                     int socketType) {
      super(shape, addresses, socketType);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      socket().connect(getAddresses());
    }

    @Override
    public void postStop() throws Exception {
      socket().close();
      super.postStop();
    }
  }

  public static class ServerStageLogic extends ZmqStageLogic {
    ServerStageLogic(Shape shape,
                     String addresses,
                     int socketType) {
      super(shape, addresses, socketType);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      socket().bind(getAddresses());
    }

    @Override
    public void postStop() throws Exception {
      socket().close();
      super.postStop();
    }
  }
}
