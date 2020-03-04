package com.github.olaplace.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public Topology createTopology(){
        StreamsBuilder builder = new StreamsBuilder();

        // 1 - Stream from Kafka's topic
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        //Topology topology = builder.build();

        KTable <String, Long> wordCounts =  wordCountInput
                // 2 - map values in lower case
                .mapValues(textLine -> textLine.toLowerCase())
                // Can be alternatively written as
                // .mapValues(String::toLowerCase)
                // 3 - Flat map values by space
                .flatMapValues(lowercaseTextLine -> Arrays.asList(lowercaseTextLine.split("\\W+")))
                // 4 - Select Key to apply a key (we want to discard the old key)
                //.selectKey((ignoreKey, word) -> word)
                .groupBy((ignoreKey, word) -> word)
                // 5 - Group by key before aggregation
                //.groupByKey()
                // 6 - Count number of occurences
                //.count(Named.as("Counts"));
                .count();

        // 7 - Write the data back to Kafka
        //wordCounts.toStream().to( "word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCountApp wordCountApp = new WordCountApp();

        KafkaStreams streams = new KafkaStreams(wordCountApp.createTopology(), config);

        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        //streams.cleanUp();
        streams.start();

        // Print the topology
        //System.out.println(streams.toString());
        // Update:
        // print the topology every 5 seconds for learning purposes
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }

        // Add a shutdown hook to close properly the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
