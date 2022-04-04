import com.rabbitmq.client.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class consumer {
    private final static String QUEUE_NAME = "Hello";
    private static ObjectPool<Connection> connPool;
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        int numThreads = 32;
        factory.setHost("172.31.87.118");
        factory.setUsername("test");
        factory.setPassword("test");
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
//        connection.addShutdownListener(new ShutdownListener() {
//            @Override
//            public void shutdownCompleted(ShutdownSignalException e) {
//                if (e.isHardError()) {
//                    System.out.println(e.getMessage());
//                    Connection conn = (Connection) e.getReference();
//                    if (!e.isInitiatedByApplication()) {
//                        Method reason = e.getReason();
//                        System.out.println("hard :" + reason);
//                    }
//                } else {
//                    Channel ch = (Channel)e.getReference();
//                    System.out.println(ch);
//                }
//            }
//        });
        ExecutorService es = Executors.newFixedThreadPool(numThreads);
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        JedisPool pool = new JedisPool(poolConfig, "172.31.91.246", 6379, 1800);
        connPool = new GenericObjectPool<>(new RMQConnectionFactory(factory));
        for (int i = 0; i < numThreads; i++) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    Channel channel = null;
                    Connection finalConnection = null;
                    try {
                        finalConnection = connPool.borrowObject();
                        channel = finalConnection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                        channel.basicQos(5);
                        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (finalConnection != null) {
                                connPool.returnObject(finalConnection);
                            }
                        } catch (Exception ignored) {
                        }
                    }
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                        System.out.println(" [x] Received '" + message + "'");
                        String[] messageArray = message.split("\n");
                        String resortId = messageArray[0];
                        String seasonId = messageArray[1];
                        String dayId = messageArray[2];
                        String skierId = messageArray[3];
                        String time = messageArray[4];
                        String liftId = messageArray[5];
                        String waitTime = messageArray[6];
                        String value = resortId + "," + seasonId + "," + dayId + ",";
                        String key = skierId;
                        Integer currentVerticalTotal = Integer.parseInt(liftId) * 10;
                        int count = 0;
                        String collectionName = "skier_" + skierId;
                        try (Jedis jedis = pool.getResource()) {
                            jedis.auth("123456");
                            // Add dayId into a redis set
//                            System.out.println(jedis);
                            jedis.sadd(collectionName, dayId);
                            // Put a skier's current day vertical amount into a redis hash. If a record exist,
                            // calculate a new value and overwrite the old one.
                            String hashName = "hash_" + collectionName;
                            long insertHash = jedis.hsetnx(hashName, dayId, currentVerticalTotal.toString());
//                            System.out.println("Insert hash return: " + insertHash);
                            if (insertHash == 0) {
                                String hashVerticalTotal = jedis.hget(hashName, dayId);
//                                System.out.println("current value is :" + hashVerticalTotal);
                                Integer overall = Integer.parseInt(hashVerticalTotal) + currentVerticalTotal;
                                jedis.hset(hashName, dayId, overall.toString());
                            }
                            // Add liftId into a redis set
                            String skierDayName = "skier_" + skierId + "_" + dayId;
                            jedis.sadd(skierDayName, liftId);
                        }
                    };
                    try {
                        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
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
