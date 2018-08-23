package no.sysco.middleware.alpakka.brave.javadsl;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.BidiShape;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import brave.Span;
import brave.Tracing;
import brave.propagation.TraceContext;

public class Brave {

  public static <T> Graph<FlowShape<T, Pair<T, TraceContext>>, NotUsed> startSpanFlow(final Tracing tracing,
                                                                                      final String spanName) {
    return Flow.fromGraph(new BraveStartSpanFlowStage<>(tracing, spanName));
  }

  public static <T> Graph<FlowShape<Pair<T, TraceContext>, T>, NotUsed> finishSpanFlow(final Tracing tracing) {
    return Flow.fromGraph(new BraveFinishSpanFlowStage<>(tracing));
  }

  public static <A, B, Mat> Graph<FlowShape<Pair<A, TraceContext>, Pair<B, TraceContext>>, Mat> childSpanFlow(final Tracing tracing,
                                                                                              final String spanName,
                                                                                              final Flow<A, B, Mat> flow) {
    return GraphDSL.create(flow, (builder, flowShape) -> {
      final BidiShape<Pair<A, TraceContext>, A, B, Pair<B, TraceContext>> bidiShape =
          builder.add(new BraveSpanBidiStage<>(tracing, spanName));

      builder.from(bidiShape.out1())
          .via(flowShape)
          .toInlet(bidiShape.in2());

      return new FlowShape<>(bidiShape.in1(), bidiShape.out2());
    });
  }
}
