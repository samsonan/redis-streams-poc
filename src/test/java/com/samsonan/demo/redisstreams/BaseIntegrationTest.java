package com.samsonan.demo.redisstreams;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.AfterClass;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
@SuppressWarnings("resource")
public class BaseIntegrationTest {

    private static final GenericContainer<?> REDIS_CONTAINER =
            new GenericContainer<>("library/redis:6-alpine")
                    .withExposedPorts(6379)
                    .waitingFor(new HostPortWaitStrategy())
                    .withReuse(true);

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @AfterEach
    @SuppressWarnings("ConstantConditions")
    void tearDown() {
        var keys = stringRedisTemplate.keys("*");
        stringRedisTemplate.delete(keys);
    }

    @BeforeAll
    public static void beforeAll() {
        REDIS_CONTAINER.start();

        initRedisClient();
    }

    @AfterClass
    public static void cleanupRedis() {
        RedisURI redisUri = RedisURI.builder()
                .withHost(REDIS_CONTAINER.getHost())
                .withPort(REDIS_CONTAINER.getFirstMappedPort())
                .build();

        var connection = RedisClient.create(redisUri).connect();
        var commands = connection.sync();
        commands.flushall(); // clean up everything
    }

    private static void initRedisClient() {
        RedisURI redisUri = RedisURI.builder()
                .withHost(REDIS_CONTAINER.getHost())
                .withPort(REDIS_CONTAINER.getFirstMappedPort())
                .build();

        RedisClient redisClient = RedisClient.create(redisUri);
    }
}
