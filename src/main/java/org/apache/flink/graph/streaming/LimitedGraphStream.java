package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Represents a limited graph stream where the stream consists solely of {@link Edge edges} and has a limited capacity.
 * <p>
 *
 * @param <K>  the key type for edge and vertex identifiers.
 * @param <EV> the value type for edges.
 * @see Edge
 */
@SuppressWarnings("serial")

public class LimitedGraphStream<K, EV> extends SimpleEdgeStream<K, EV> {
    private final double limit;

    public LimitedGraphStream(DataStream<Edge<K, EV>> edges, StreamExecutionEnvironment context, double limit) {
        super(edges, context);
        this.limit = limit;
    }

    public LimitedGraphStream(DataStream<Edge<K, EV>> edges, AscendingTimestampExtractor<Edge<K, EV>> timeExtractor, StreamExecutionEnvironment context, double limit) {
        super(edges, timeExtractor, context);
        this.limit = limit;

    }

    //TODO: write tests

    /**
     * Builds the neighborhood state by creating adjacency lists.
     * Neighborhoods are currently built using a TreeSet with considering probability.
     *
     * @param directed if true, only the out-neighbors will be stored
     *                 otherwise both directions are considered
     * @return a stream of Tuple4, where the first 2 fields identify the edge processed
     * and the third field is the adjacency list that was updated by processing this edge
     * and the last field is the probability that triangle.
     */
    public DataStream<Tuple4<K, K, TreeSet<K>, Double>> buildLimitedNeighborhood(boolean directed) {
        DataStream<Edge<K, EV>> edges = this.getEdges();
        if (!directed) {
            edges = this.undirected().getEdges();
        }
        return edges.keyBy(0).flatMap(new LimitedBuildNeighborhoods<>(this.limit));
    }


    private static final class LimitedBuildNeighborhoods<K, EV> implements FlatMapFunction<Edge<K, EV>, Tuple4<K, K, TreeSet<K>, Double>> {
        private static final long SEED = 1919; // set default seed
        private final double limit; // limit for every worker
        private double edgeCount = 0; // number of edges
        Random generator = new Random(SEED); // generate equal random number
        Map<K, TreeSet<K>> neighborhoods = new HashMap<>();
        Tuple4<K, K, TreeSet<K>, Double> outTuple = new Tuple4<>();


        public LimitedBuildNeighborhoods(double limit) {
            this.limit = limit;
        }

        public void flatMap(Edge<K, EV> e, Collector<Tuple4<K, K, TreeSet<K>, Double>> out) {
            edgeCount++; // increase received edges count

            TreeSet<K> tree;
            if (neighborhoods.containsKey(e.getSource())) {
                tree = neighborhoods.get(e.getSource());
            } else {
                tree = new TreeSet<>();
            }
            tree.add(e.getTarget());

            double coefficient = 1.0 / sampleProbability();


            outTuple.setField(e.getSource(), 0);
            outTuple.setField(e.getTarget(), 1);
            outTuple.setField(tree, 2);
            outTuple.setField(coefficient, 3);

            if (neighborhoods.size() < limit) {
                neighborhoods.put(e.getSource(), tree);
            } else if (coin()) {
                neighborhoods.put(e.getSource(), tree);

                // remove randomly
                List<K> keyList = new ArrayList<>(neighborhoods.keySet());

                int size = keyList.size();
                int randIdx = generator.nextInt(size);

                neighborhoods.remove(keyList.get(randIdx));
            }

            out.collect(outTuple);
        }

        private boolean coin() {
            return limit / edgeCount > Math.random();
        }


        private double sampleProbability() {
            double num = limit * (limit - 1);
            double dnom = edgeCount * (edgeCount - 1);
            return Math.min(1.0, num / (dnom));
        }
    }
}
