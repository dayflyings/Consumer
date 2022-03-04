import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.*;

public class consumer {
    private final static String QUEUE_NAME = "Hello";
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        int numThreads = 32;
        factory.setHost("44.201.198.137");
        factory.setUsername("test");
        factory.setPassword("test");
        Connection connection = null;
        ConcurrentHashMap<String, String> record = new ConcurrentHashMap<>();
        connection = factory.newConnection();
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            Connection finalConnection = connection;
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Channel channel = finalConnection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                            System.out.println(" [x] Received '" + message + "'");
                            String[] messageArray = message.split("\n");
                            String resortId = messageArray[0];
                            String seasonId = messageArray[1];
                            String dayId = messageArray[2];
                            String skierId = messageArray[3];
                            String value = resortId + "," + seasonId + "," + dayId + "," ;
                            String key = skierId;
                            int count = 0;
                            while (record.containsKey(key)) {
                                count++;
                                key = skierId + "_" + count;
                            }
                            record.put(key, value);
                        };
                        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        es.shutdown();
        while (!es.awaitTermination(10, TimeUnit.SECONDS)) {
            System.out.println("Waiting for all threads finished");
        }
    }
}
