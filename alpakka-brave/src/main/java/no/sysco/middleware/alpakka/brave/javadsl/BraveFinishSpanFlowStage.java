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
import brave.Tracing;
import brave.propagation.TraceContext;

class BraveFinishSpanFlowStage<T> extends GraphStage<FlowShape<Pair<T, TraceContext>, T>> {

  private final Tracing tracing;

  private final Inlet<Pair<T, TraceContext>> in = Inlet.create("BraveFinishSpanFlow.in");
  private final Outlet<T> out = Outlet.create("BraveFinishSpanFlow.out");

  BraveFinishSpanFlowStage(Tracing tracing) {
    this.tracing = tracing;
  }

  public FlowShape<Pair<T, TraceContext>, T> shape() {
    return new FlowShape<>(in, out);
  }

  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    return new GraphStageLogic(shape()) {

      {
        setHandler(in, new AbstractInHandler() {
          public void onPush() {
            final Pair<T, TraceContext> grab = grab(in);
            final TraceContext traceContext = grab.second();
            tracing.tracer().toSpan(traceContext).finish();
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
