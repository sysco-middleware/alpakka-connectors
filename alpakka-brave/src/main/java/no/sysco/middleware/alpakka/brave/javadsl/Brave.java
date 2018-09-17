package no.sysco.middleware.alpakka.brave.javadsl;

import akka.NotUsed;
import akka.japi.Pair;
import akka.stream.BidiShape;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import brave.Tracing;
import brave.propagation.TraceContext;

/**
 * Brave instrumentation for Akka Streams.
 * It creates and finish traces, and allow wrapping {@link Flow}s inside {@link brave.Span}s, to
 * measure latency.
 */
public class Brave {

  /**
   * Creates a {@link Flow} that pairs input element with a started trace context, that can be propagated
   * and must be closed before completing a stream transaction to be reported. See `finishSpanFlow`.
   */
  public static <T> Flow<T, Pair<T, TraceContext>, NotUsed> startSpanFlow(final Tracing tracing,
                                                                          final String spanName) {
    return Flow.fromGraph(new BraveStartSpanFlowStage<>(tracing, spanName));
  }

  /**
   * Creates a {@link Flow} that takes a trace context started before, and finish it.
   */
  public static <T> Graph<FlowShape<Pair<T, TraceContext>, T>, NotUsed> finishSpanFlow(final Tracing tracing) {
    return Flow.fromGraph(new BraveFinishSpanFlowStage<>(tracing));
  }

  /**
   * Creates a {@link Flow} that wraps another {@link Flow} using a {@link BidiShape}. Creates
   * a Child Span based on a Parent trace that will be received as input. After completing
   * Flow execution, it continues parent trace context.
   */
  public static <A, B, Mat> Graph<FlowShape<Pair<A, TraceContext>, Pair<B, TraceContext>>, Mat> childSpanFlowWithTraceContext(final Tracing tracing,
                                                                                                                              final String spanName,
                                                                                                                              final Flow<Pair<A, TraceContext>, B, Mat> flow) {
    return GraphDSL.create(flow, (builder, flowShape) -> {
      final BidiShape<Pair<A, TraceContext>, Pair<A, TraceContext>, B, Pair<B, TraceContext>> bidiShape =
          builder.add(new BraveSpanBidiFlowWithTraceContextStage<>(tracing, spanName));

      builder.from(bidiShape.out1())
          .via(flowShape)
          .toInlet(bidiShape.in2());

      return new FlowShape<>(bidiShape.in1(), bidiShape.out2());
    });
  }

  /**
   * Creates a {@link Flow} that wraps another {@link Flow} using a {@link BidiShape}. Creates
   * a Child Span based on a Parent trace that will be received as input. After completing
   * Flow execution, it continues parent trace context, but Child Span is propagated internally to
   * wrapped Flow for further manipulation.
   */
  public static <A, B, Mat> Graph<FlowShape<Pair<A, TraceContext>, Pair<B, TraceContext>>, Mat> childSpanFlow(final Tracing tracing,
                                                                                                              final String spanName,
                                                                                                              final Flow<A, B, Mat> flow) {
    return GraphDSL.create(flow, (builder, flowShape) -> {
      final BidiShape<Pair<A, TraceContext>, A, B, Pair<B, TraceContext>> bidiShape =
          builder.add(new BraveSpanBidiFlowStage<>(tracing, spanName));

      builder.from(bidiShape.out1())
          .via(flowShape)
          .toInlet(bidiShape.in2());

      return new FlowShape<>(bidiShape.in1(), bidiShape.out2());
    });
  }
}
