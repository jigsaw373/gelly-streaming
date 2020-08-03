package org.apache.flink.graph.streaming.example;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.LimitedGraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on Tri-Fly algorithm https://web2.qatar.cmu.edu/~mhhammou/triflyPAKDD2018.pdf.
 */

public class BroadcastTriFlyTriangleCount {
    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();

        env.setParallelism(32);

        LimitedGraphStream<Integer, NullValue> edges = getGraphStream(env);

        DataStream<Tuple2<Integer, Double>> result =
                edges.buildLimitedNeighborhood(false)
                        .map(new ProjectCanonicalEdges())
                        .keyBy(0, 1).flatMap(new IntersectNeighborhoods())
                        .keyBy(0).flatMap(new SumAndEmitCounters());

        if (resultPath != null) {
            result.writeAsText(resultPath);
        } else {
            result.print();
        }

        env.execute("Broadcast Tri-Fly Triangle Count");
    }

    // *** Transformation Methods *** //

    /**
     * Receives 2 tuples from the same edge (src + target) and intersects the attached neighborhoods.
     * For each common neighbor, increase local and global counters.
     */
    public static final class IntersectNeighborhoods implements
            FlatMapFunction<Tuple4<Integer, Integer, TreeSet<Integer>, Double>, Tuple2<Integer, Double>> {

        Map<Tuple2<Integer, Integer>, TreeSet<Integer>> neighborhoods = new HashMap<>();

        public void flatMap(Tuple4<Integer, Integer, TreeSet<Integer>, Double> t, Collector<Tuple2<Integer, Double>> out) {
            //intersect neighborhoods and emit local and global counters
            Tuple2<Integer, Integer> key = new Tuple2<>(t.f0, t.f1);
            if (neighborhoods.containsKey(key)) {
                // this is the 2nd neighborhood => intersect
                TreeSet<Integer> t1 = neighborhoods.remove(key);
                TreeSet<Integer> t2 = t.f2;
                double counter = 0;
                if (t1.size() < t2.size()) {
                    // iterate t1 and search t2
                    for (int i : t1) {
                        if (t2.contains(i)) {
                            counter += t.f3;
                            out.collect(new Tuple2<>(i, t.f3));
                        }
                    }
                } else {
                    // iterate t2 and search t1
                    for (int i : t2) {
                        if (t1.contains(i)) {
                            counter += t.f3;
                            out.collect(new Tuple2<>(i, t.f3));
                        }
                    }
                }
                if (counter > 0) {
                    //emit counter for srcID, trgID, and total
                    out.collect(new Tuple2<>(t.f0, counter));
                    out.collect(new Tuple2<>(t.f1, counter));
                    // -1 signals the total counter
                    out.collect(new Tuple2<>(-1, counter));
                }
            } else {
                // first neighborhood for this edge: store and wait for next
                neighborhoods.put(key, t.f2);
            }
        }
    }

    /**
     * Sums up and emits local and global counters.
     */
    public static final class SumAndEmitCounters implements FlatMapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
        Map<Integer, Double> counts = new HashMap<>();

        public void flatMap(Tuple2<Integer, Double> t, Collector<Tuple2<Integer, Double>> out) {
            if (counts.containsKey(t.f0)) {
                double newCount = counts.get(t.f0) + t.f1;
                counts.put(t.f0, newCount);
                out.collect(new Tuple2<>(t.f0, newCount));
            } else {
                counts.put(t.f0, t.f1);
                out.collect(new Tuple2<>(t.f0, t.f1));
            }
        }
    }

    public static final class ProjectCanonicalEdges implements
            MapFunction<Tuple4<Integer, Integer, TreeSet<Integer>, Double>, Tuple4<Integer, Integer, TreeSet<Integer>, Double>> {
        @Override
        public Tuple4<Integer, Integer, TreeSet<Integer>, Double> map(Tuple4<Integer, Integer, TreeSet<Integer>, Double> t) {
            int source = Math.min(t.f0, t.f1);
            int trg = Math.max(t.f0, t.f1);
            t.setField(source, 0);
            t.setField(trg, 1);
            return t;
        }
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = null;
    private static String resultPath = null;
    private static long workerLimit = 10000;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 3) {
                System.err.println("Usage: BroadcastTriangleCount <input edges path> <result path> <worker limit>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            resultPath = args[1];
            workerLimit = Integer.parseInt(args[2]);

        } else {
            System.out.println("Executing BroadcastTriangleCount example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: BroadcastTriangleCount <input edges path> <result path> <worker limit>");
        }
        return true;
    }


    @SuppressWarnings("serial")
    private static LimitedGraphStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {
        if (fileOutput) {
            return new LimitedGraphStream<>(env.readTextFile(edgeInputPath)
                    .flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
                        @Override
                        public void flatMap(String s, Collector<Edge<Integer, NullValue>> out) {
                            String[] fields = s.split("\\s");
                            if (!fields[0].equals("%")) {
                                int src = Integer.parseInt(fields[0]);
                                int trg = Integer.parseInt(fields[1]);
                                out.collect(new Edge<>(src, trg, NullValue.getInstance()));
                            }
                        }
                    }), env, workerLimit);
        }

        return new LimitedGraphStream<>(env.fromElements(
                new Edge<>(1, 2, NullValue.getInstance()),
                new Edge<>(2, 3, NullValue.getInstance()),
                new Edge<>(2, 6, NullValue.getInstance()),
                new Edge<>(5, 6, NullValue.getInstance()),
                new Edge<>(1, 4, NullValue.getInstance()),
                new Edge<>(5, 3, NullValue.getInstance()),
                new Edge<>(3, 4, NullValue.getInstance()),
                new Edge<>(3, 6, NullValue.getInstance()),
                new Edge<>(1, 3, NullValue.getInstance())), env, workerLimit);
    }
}
