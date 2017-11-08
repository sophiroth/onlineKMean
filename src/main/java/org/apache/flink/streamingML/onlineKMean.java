package org.apache.flink.streamingML;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.LinkedList;
import java.util.Random;
import java.util.Scanner;

public class onlineKMean {
    public static double distance(LinkedList<Double> a, LinkedList<Double> b) {
        double dist = 0.0;
        for (int i = 0; i < a.size(); i++) {
            double x = a.get(i);
            double y = b.get(i);
            dist += (x - y) * (x - y);
        }
        return Math.sqrt(dist);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.getConfig().setGlobalJobParameters(parameters);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer08 consumer = new FlinkKafkaConsumer08<>("3droad", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();

        DataStream<String> raw = see.addSource(consumer);

        DataStream<Tuple2<Integer, LinkedList<Double>>> parsed = raw.map(
                new MapFunction<String, Tuple2<Integer, LinkedList<Double>>>() {
            @Override
            public Tuple2<Integer, LinkedList<Double>> map(String line) {
                Scanner sc = new Scanner(line);
                LinkedList<Double> list = new LinkedList<Double>();
                for (String number: line.split(",")) {
                    list.add(Double.parseDouble(number));
                }
                Tuple2<Integer, LinkedList<Double>> tuple2 = new Tuple2<Integer, LinkedList<Double>>();
                tuple2.f0 = 1;
                tuple2.f1 = list;
                return tuple2;
            }
        });


        DataStream<Tuple2<LinkedList<Double>, Integer>> classified = parsed.keyBy(0).flatMap(
                new RichFlatMapFunction<Tuple2<Integer, LinkedList<Double>>, Tuple2<LinkedList<Double>, Integer>>() {
            private transient ValueState<Tuple2<Integer, LinkedList<LinkedList<Double>>>> centroids;
            private transient ValueState<Tuple2<Integer, Long>> counts;
            private ParameterTool parameters; 
            // private int numFeatures;
            private int k;
            @Override
            public void flatMap(
                    Tuple2<Integer, LinkedList<Double>> input, Collector<Tuple2<LinkedList<Double>, Integer>> out) 
            throws Exception {
                Tuple2<Integer, LinkedList<LinkedList<Double>>> curCentroids = centroids.value();
                curCentroids.f1.add(input.f1);
                centroids.update(curCentroids);
                LinkedList<Double> temp = new LinkedList<Double>();
                out.collect(new Tuple2<LinkedList<Double>, Integer>(input.f1, 1));
                centroids.clear();
            }

            @Override
            public void open(Configuration conf) {
                ValueStateDescriptor<Tuple2<Integer, LinkedList<LinkedList<Double>>>> descriptorCentroids = 
                    new ValueStateDescriptor<Tuple2<Integer, LinkedList<LinkedList<Double>>>>(
                            "centroids", 
                            TypeInformation.of(new TypeHint<Tuple2<Integer, LinkedList<LinkedList<Double>>>>() {}), 
                            Tuple2.of(1, new LinkedList<LinkedList<Double>>()));
                
                ValueStateDescriptor<Tuple2<Integer, Long>> descriptorCounts = 
                    new ValueStateDescriptor<>(
                            "counts", 
                            TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {}), 
                            Tuple2.of(0, 0L));

                centroids = getRuntimeContext().getState(descriptorCentroids);
                counts = getRuntimeContext().getState(descriptorCounts);
                // numFeatures = parameters.getRequired("numFeatures");
                parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                k = parameters.getInt("k");
            }
            
        }).setParallelism(2);

        DataStream<String> results = classified.map(new MapFunction<Tuple2<LinkedList<Double>, Integer>, String>() {
            @Override
            public String map(Tuple2<LinkedList<Double>, Integer> temp) {
                return temp.toString();
            }
        });
        results.addSink(new FlinkKafkaProducer08<>("localhost:9092", "output", new SimpleStringSchema()));
        see.execute();
    }
}
