package org.apache.flink.graph.streaming.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.graph.streaming.Worker;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;

import java.io.Serializable;
import java.util.Random;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on Tri-Fly algorithm https://web2.qatar.cmu.edu/~mhhammou/triflyPAKDD2018.pdf.
 */
public class Trifly implements ProgramDescription, Serializable {

    public static void main(String[] args) throws Exception {

        // Set up the environment
        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

        // Count triangles
        DataStream<Tuple2<Long, Double>> triangles = edges
                .broadcast()
                .flatMap(new Worker<>(workerCapacity, (int) System.currentTimeMillis()))
                .partitionCustom(new CustomPartitioner(), new CustomKeySelector())
                .keyBy(0).reduce((t1, t2) -> new Tuple2<>(t1.f0, t1.f1 + t2.f1))
                .map(new Normalizer(env.getParallelism()));

        // Emit the results
        if (fileOutput) {
            triangles.writeAsCsv(outputPath);
        } else {
            triangles.print();
        }

        env.execute("Trifly Triangle Count");
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = null;
    private static String outputPath = null;
    private static int workerCapacity = 1000;

    private static boolean parseParameters(String[] args) {
        if (args.length > 0) {
            if (args.length != 3) {
                System.err.println("Usage: Trifly <input edges path> <output path> <worker capacity>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            workerCapacity = Integer.parseInt(args[2]);
        } else {
            System.out.println("Executing Trifly example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: Trifly <input edges path> <output path> <worker capacity>");
        }
        return true;
    }

    @SuppressWarnings("serial")
    private static DataStream<Edge<Long, NullValue>> getEdgesDataSet(StreamExecutionEnvironment env) {
        if (fileOutput) {
            return env.readTextFile(edgeInputPath)
                    .map(new MapFunction<String, Edge<Long, NullValue>>() {
                        @Override
                        public Edge<Long, NullValue> map(String s) throws Exception {
                            String[] fields = s.split("\\t");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1]);
                            return new Edge<>(src, trg, NullValue.getInstance());
                        }
                    });
        }

        return env.generateSequence(0, 999).flatMap(
                new FlatMapFunction<Long, Edge<Long, NullValue>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
                        out.collect(new Edge<>(key, (key + 2) % 1000, NullValue.getInstance()));
                        out.collect(new Edge<>(key, (key + 4) % 1000, NullValue.getInstance()));
                    }
                });
    }

    @Override
    public String getDescription() {
        return "Trifly Triangle Count";
    }

    public static final class Normalizer implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
        private final int parallelism;

        public Normalizer(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> t) throws Exception {
            return new Tuple2<>(t.f0, t.f1 / parallelism);
        }
    }

    private static class CustomPartitioner implements Partitioner<Tuple2<Long, Double>> {
        @Override
        public int partition(Tuple2<Long, Double> key, int numPartitions) {
            if (key.f0 == -1) {
                return new Random().nextInt(numPartitions);
            }
            return (int) (key.f0 % (long) numPartitions);
        }
    }

    private static class CustomKeySelector implements KeySelector<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public Tuple2<Long, Double> getKey(Tuple2<Long, Double> value) throws Exception {
            return value;
        }
    }
}

