package org.apache.flink.graph.streaming;

import java.util.*;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;
import org.apache.flink.api.java.tuple.Tuple2;


public class Worker<K, EV> implements FlatMapFunction<Edge<K, EV>, Tuple2<K, Double>> {
    // maximum number of edges to be stored in the worker
    private final long MAX;

    // list of sampled edges
    private List<Edge<K, EV>> samples;

    // count of received edges
    private long streamSize;

    // sampled graph
    private Map<K, HashSet<K>> graph;

    // random number generator
    Random generator;

    public Worker(long limit, int seed) {
        MAX = limit;
        samples = new ArrayList<>();
        graph = new HashMap<>();
        generator = new Random(seed);
    }


    @Override
    public void flatMap(Edge<K, EV> edge, Collector<Tuple2<K, Double>> out) throws Exception {
        K source = edge.getSource();
        K target = edge.getTarget();

        // ignore self loop
        if (source.equals(target))
            return;

        // update count of triangles
        updateCount(edge, out);

        if (isSampled()) {
            if (streamSize < MAX) {
                samples.add(edge);
            } else {
                replaceSample(edge);
            }

            graph.computeIfAbsent(source, k -> new HashSet<>());
            graph.computeIfAbsent(target, k -> new HashSet<>());

            graph.get(source).add(target);
            graph.get(target).add(source);
        }

        streamSize++;
    }

    private void updateCount(Edge<K, EV> edge, Collector<Tuple2<K, Double>> out) {
        K source = edge.getSource();
        K target = edge.getTarget();

        HashSet<K> sourceNeighbors = graph.get(source);
        HashSet<K> targetNeighbors = graph.get(target);

        if (sourceNeighbors == null || targetNeighbors == null)
            return;

        double sum = 0;
        for (K src : sourceNeighbors) {
            if (targetNeighbors.contains(src)) {
                double count = count();
                sum += count;

                out.collect(new Tuple2<>(src, count));
            }
        }

        if (sum > 0) {
            out.collect(new Tuple2<>(source, sum));
            out.collect(new Tuple2<>(target, sum));
            out.collect(new Tuple2<>((K) new Long(-1), sum));
        }
    }

    private boolean isSampled() {
        return (streamSize < MAX) || coin();
    }

    private boolean coin() {
        long l = MAX / (streamSize + 1);
        return Math.random() < l;
    }

    private void replaceSample(Edge<K, EV> edge) {
        // remove randomly
        Edge<K, EV> sample = samples.remove(generator.nextInt(samples.size()));

        // remove neighbors
        graph.get(sample.getSource()).remove(sample.getTarget());
        graph.get(sample.getTarget()).remove(sample.getSource());

        samples.add(edge);
    }

    private double count() {
        double curSampleNum = Math.min(MAX, streamSize);
        double prob = (curSampleNum / streamSize * (curSampleNum - 1) / (streamSize - 1));

        return 1.0 / prob;
    }
}
