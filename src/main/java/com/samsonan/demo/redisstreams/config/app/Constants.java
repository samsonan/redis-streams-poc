package com.samsonan.demo.redisstreams.config.app;

public final class Constants {

    public static final int MAX_LOCK_TRYING_TIME_MILLIS = 30_000;
    public static final int INITIAL_LOCKING_INTERVAL = 500;
    public static final double RETRY_MULTIPLIER = 1.5;
    public static final double RETRY_RANDOMIZATION_FACTOR = 0.5;
    public static final int LOCKING_ATTEMPTS = 5;
    public static final int MAX_LOCKING_TIME_SECONDS = 30;
    public static final String LOCK_KEY = "consumer.config.lock";
    public static final String CONSUMER_STREAM_KEY = "consumer.stream.key";
    public static final String CONSUMER_UPDATED_AT = "consumer.updated.at";
}
