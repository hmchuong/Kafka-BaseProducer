import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.*;

/**
 * Created by hmchuong on 27/06/2017.
 */
public class BaseProducer implements Runnable {

    private KafkaProducer producer;
    private String timezoneTopic;
    private String eventTopic;
    private String filename;

    public BaseProducer(String args[]){
        ArgumentParser argumentParser = new ArgumentParser();
        argumentParser.buildArgument(args);
        Properties config = new Properties();
        config.put("bootstrap.servers",argumentParser.kafkaHost);
        config.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "all");
        producer = new KafkaProducer(config);
        this.eventTopic = argumentParser.mainTopic;
        this.timezoneTopic = argumentParser.timezoneTopic;
        this.filename = argumentParser.filename;
    }

    @Override
    public void run() {
        List<String> lines = new ArrayList();
        try(BufferedReader br = new BufferedReader(new FileReader(filename))){
            String sCurrentLine;
            while ((sCurrentLine = br.readLine())!=null){
                lines.add(sCurrentLine);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < lines.size(); i++){
            String topic = this.timezoneTopic;
            if (lines.get(i).indexOf("timezone") == -1){
                topic = this.eventTopic;
            }
            ProducerRecord record;
            record = new ProducerRecord<>(topic, lines.get(i));
//            Future<RecordMetadata> future = producer.send(record);
//            try {
//                RecordMetadata metadata = future.get();
//                System.out.println("Send successfully to " + metadata.topic() + " at partition " + String.valueOf(metadata.partition()));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
            producer.send(record, (recordMetadata, e) -> {
                if (e!= null){
                    System.out.println("Send record failed: "+record.toString());
                }else{
                    System.out.println("Send successfully to "+recordMetadata.topic()+" at partition "+String.valueOf(recordMetadata.partition()));
                }
            });
            if (i%2 == 0) {
                try {
                    Thread.sleep(500 + (int)(Math.random() * 3000) );
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        producer.flush();
    }

    /**
     * Class for JCommander
     */
    private class ArgumentParser {
        @Parameter(names = {"-kafHost","-kh"}, description = "bootstrap.servers: host of kafka.")
        private String kafkaHost = "localhost:9092";

        @Parameter(names = {"-timezoneTopic", "-tt"},description = "Timezone topic.", required = true)
        private String timezoneTopic;

        @Parameter(names = {"-eventTopic","-et"},description = "Event topic.", required = true)
        private String mainTopic;

        @Parameter(names = {"-file","-f"},description = "File contains messages. Each line is one message", required = true)
        private String filename;

        @Parameter(names = {"--help","--h"}, help = true)
        private boolean help;

        private void buildArgument(String[] argv){
            JCommander jcommander = JCommander.newBuilder().build();
            jcommander.addObject(this);
            try {
                jcommander.parse(argv);
            }catch (ParameterException e){
                System.out.println(e.getMessage());
                e.getJCommander().usage();
                System.exit(1);
            }

            // Show helper
            if (this.help){
                jcommander.usage();
                System.exit(1);
            }
        }
    }

    public static void main(String[] args){
        (new BaseProducer(args)).run();
    }
}
