package no.sysco.middleware.alpakka.brave.javadsl;

import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.BidiShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import brave.Span;
import brave.Tracer;
import brave.Tracing;

class BraveSpanBidiStage<A, B> extends GraphStage<BidiShape<Pair<A, Span>, A, B, Pair<B, Span>>> {

  private final Tracing tracing;
  private final String name;

  private final Inlet<Pair<A, Span>> in1 = Inlet.create("BraveSpanBidi.in1");
  private final Outlet<A> out1 = Outlet.create("BraveSpanBidi.out1");
  private final Inlet<B> in2 = Inlet.create("BraveSpanBidi.in2");
  private final Outlet<Pair<B, Span>> out2 = Outlet.create("BraveSpanBidi.out2");

  BraveSpanBidiStage(Tracing tracing, String name) {
    this.tracing = tracing;
    this.name = name;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    return new GraphStageLogic(shape()) {
      final Tracer tracer = tracing.tracer();

      Span current;
      Span parent;

      {

        setHandler(in1, new AbstractInHandler() {
          @Override
          public void onPush() {
            final Pair<A, Span> grab = grab(in1);
            parent = grab.second();
            current = tracer.newChild(grab.second().context()).name(name).start();
            push(out1, grab.first());
          }
        });

        setHandler(out1, new AbstractOutHandler() {
          @Override
          public void onPull() {
            pull(in1);
          }
        });

        setHandler(in2, new AbstractInHandler() {
          @Override
          public void onPush() {
            current.finish();
            push(out2, Pair.create(grab(in2), parent));
          }
        });

        setHandler(out2, new AbstractOutHandler() {
          @Override
          public void onPull() {
            pull(in2);
          }
        });
      }
    };
  }

  @Override
  public BidiShape<Pair<A, Span>, A, B, Pair<B, Span>> shape() {
    return new BidiShape<>(in1, out1, in2, out2);
  }
}
