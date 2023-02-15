package io.github.lcn29.snowflake.id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <pre>
 *
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-12 22:00
 */
public class SnowFlakeIdGenerator {
    private static final Logger log = LoggerFactory.getLogger(SnowFlakeIdGenerator.class);
    static final long epoch = 1558738740000L;
    private long workerId;
    private WorkerIdAssigner workerIdAssigner;
    private static final long failoverBits = 2L;
    private static final long workerIdBits = 8L;
    private static final long maxFailover = 3L;
    private static final long maxWorkerId = 255L;
    private static final long sequenceBits = 12L;
    private static final long workerIdShift = 12L;
    private static final long failoverShift = 20L;
    private static final long timestampLeftShift = 22L;
    private static final long sequenceMask = 4095L;
    private Map<Long, Long> failoverLastTimestampMap = new ConcurrentHashMap();
    private long failover = 0L;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public SnowFlakeIdGenerator(int workerId) {
        for(long i = 0L; i <= 3L; ++i) {
            this.failoverLastTimestampMap.put(i, -1L);
        }

        this.validateWorkerId((long)workerId);
        this.workerId = (long)workerId;
    }

    public SnowFlakeIdGenerator(WorkerIdAssigner workerIdAssigner) {
        for(long i = 0L; i <= 3L; ++i) {
            this.failoverLastTimestampMap.put(i, -1L);
        }

        Objects.requireNonNull(workerIdAssigner);
        this.workerIdAssigner = workerIdAssigner;
    }

    private void validateWorkerId(long workerId) {
        if (workerId > 255L || workerId < 0L) {
            throw new IllegalArgumentException(String.format("Worker Id can't be greater than %d or less than 0", 255L));
        }
    }

    private long getWorkerIdForGenerate() {
        if (this.workerIdAssigner != null) {
            long workerId = (long) 123; //this.workerIdAssigner.assign();
            this.validateWorkerId(workerId);
            return workerId;
        } else {
            return this.workerId;
        }
    }

    public synchronized Long generate() {
        long currentTime = this.timeGen();
        long workerId = this.getWorkerIdForGenerate();
        if (currentTime < this.lastTimestamp) {
            this.tryUseFailover(currentTime);
        }

        if (currentTime == this.lastTimestamp) {
            this.sequence = this.sequence + 1L & 4095L;
            if (this.sequence == 0L) {
                currentTime = this.tillNextMillis(this.lastTimestamp);
            }
        } else {
            this.sequence = (long)(new Random()).nextInt(64);
        }

        this.lastTimestamp = currentTime;
        this.failoverLastTimestampMap.put(this.failover, currentTime);
        Long nextId = currentTime - 1558738740000L << 22 | this.failover << 20 | workerId << 12 | this.sequence;
        return nextId;
    }

    private long tillNextMillis(final long lastTimestamp) {
        long timestamp;
        for(timestamp = this.timeGen(); timestamp <= lastTimestamp; timestamp = this.timeGen()) {
        }

        return timestamp;
    }

    private void tryUseFailover(long currentTime) {
        Iterator var3 = this.failoverLastTimestampMap.entrySet().iterator();

        Map.Entry entry;
        do {
            if (!var3.hasNext()) {
                throw new IllegalStateException(String.format("Clock moved backwards. Refusing to generate id for %d milliseconds", this.lastTimestamp - currentTime));
            }

            entry = (Map.Entry)var3.next();
        } while((Long)entry.getValue() > currentTime);

        log.warn("Clock moved backwards. Failover successfully.");
        this.lastTimestamp = (Long)entry.getValue();
        this.failover = (Long)entry.getKey();
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

}
