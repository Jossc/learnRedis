package com.redis.queue;

import redis.clients.jedis.Jedis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.lang.reflect.Type;
import java.util.Set;
import java.util.UUID;

/**
 * @author Hikari
 * @version 1.0.0
 * @ClassName RedisDelayingQueue.java
 * @createTime 2021年03月23日 21:40:00
 */
public class RedisDelayingQueue<T> {

    static class TaskItem<T> {
        public String id;
        public T msg;
    }

    /**
     * 添加序列化对象
     */
    private Type TaskType = new TypeReference<TaskItem<T>>() {
    }.getType();

    private static Long defTime = 5000L;
    private Jedis jedis;
    private String queueKey;

    public RedisDelayingQueue(Jedis jedis, String queueKey) {
        this.jedis = jedis;
        this.queueKey = queueKey;
    }

    public void delay(T msg) {
        TaskItem<T> task = new TaskItem();
        task.id = UUID.randomUUID().toString();
        task.msg = msg;
        String jsonValue = JSON.toJSONString(task);
        jedis.zadd(queueKey, System.currentTimeMillis() + defTime, jsonValue);
    }

    public void loop() {
        while (!Thread.interrupted()) {
            Set<String> values = jedis.zrangeByScore(queueKey, 0,
                    System.currentTimeMillis(), 0, 1);
            if (values.isEmpty()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
                continue;
            }
            String s = values.iterator().next();
            if (jedis.zrem(queueKey, s) > 0) {
                TaskItem<T> task = JSON.parseObject(s, TaskType);
                this.handleMsg(task.msg);
            }
        }
    }

    public void handleMsg(T msg) {
        System.out.println(msg);
    }

    public static void main(String[] args) {
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        RedisDelayingQueue<String> queue = new RedisDelayingQueue<>(jedis, "learn-redis-queue");
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                queue.delay("code-xiaoPang :" + i);
            }
        });
        Thread consumer = new Thread(() -> queue.loop());
        producer.start();
        consumer.start();
        try {
            producer.join();
            Thread.sleep(6000);
            consumer.interrupt();
            consumer.join();
        } catch (InterruptedException e) {
        }
    }
}
