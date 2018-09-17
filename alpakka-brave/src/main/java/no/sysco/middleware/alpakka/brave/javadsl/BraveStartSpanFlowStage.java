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
import brave.propagation.TraceContext;

class BraveStartSpanFlowStage<T> extends GraphStage<FlowShape<T, Pair<T, TraceContext>>> {

  private final Tracing tracing;
  private final String name;

  private final Inlet<T> in = Inlet.create("BraveStartSpanFlow.in");
  private final Outlet<Pair<T, TraceContext>> out = Outlet.create("BraveStartSpanFlow.out");

  BraveStartSpanFlowStage(Tracing tracing, String name) {
    this.tracing = tracing;
    this.name = name;
  }

  public FlowShape<T, Pair<T, TraceContext>> shape() {
    return new FlowShape<>(in, out);
  }

  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    return new GraphStageLogic(shape()) {

      {
        final Tracer tracer = tracing.tracer();

        setHandler(in, new AbstractInHandler() {
          public void onPush() {
            Span span = tracer.newTrace().name(name).start();
            push(out, Pair.create(grab(in), span.context()));
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
