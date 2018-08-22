package no.sysco.middleware.alpakka.brave.javadsl;

import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import brave.Span;
import brave.Tracer;
import brave.Tracing;

class BraveFinishSpanFlowStage<T> extends GraphStage<FlowShape<Pair<T, Span>, T>> {

  private final Tracing tracing;

  private final Inlet<Pair<T, Span>> in = Inlet.create("BraveFinishSpanFlow.in");
  private final Outlet<T> out = Outlet.create("BraveFinishSpanFlow.out");

  BraveFinishSpanFlowStage(Tracing tracing) {
    this.tracing = tracing;
  }

  public FlowShape<Pair<T, Span>, T> shape() {
    return new FlowShape<>(in, out);
  }

  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    return new GraphStageLogic(shape()) {

      {
        final Tracer tracer = tracing.tracer();

        setHandler(in, new AbstractInHandler() {
          public void onPush() {
            final Pair<T, Span> grab = grab(in);
            grab.second().finish();
            push(out, grab.first());
          }
        });

        setHandler(out, new AbstractOutHandler() {
          @Override
          public void onPull() {
            pull(in);
          }
        });
      }

    };
  }
}
