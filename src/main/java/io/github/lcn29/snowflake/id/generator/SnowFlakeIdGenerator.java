package io.github.lcn29.snowflake.id.generator;

import io.github.lcn29.snowflake.id.Constants;
import io.github.lcn29.snowflake.id.assigner.WorkerIdAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Objects;

/**
 * <pre>
 * 雪花 id 生成器
 * long 64 位
 * 1:     1 位 固定为 0
 * 2-42:  41位 时间戳
 * 43-44: 2 位 机房号, 这里用来存储进程号了
 * 45-52: 8 位 机器号
 * 53-64: 12位 同一毫秒内递增的序列号
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-18 22:31
 */
public class SnowFlakeIdGenerator implements IdGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SnowFlakeIdGenerator.class);

    /**
     * 毫秒内自增位数, 12 位
     */
    private final static int SEQUENCE_BITS = 12;

    /**
     * work id 位数, 8 位
     */
    private final static int WORK_ID_BITS = 8;

    /**
     * 进程 id, 2 位
     */
    private final static int PROCESS_ID_BITS = 2;

    /**
     * work id 左移位数 12 位
     */
    private final static int WORKER_ID_LEFT_SHIFT = SEQUENCE_BITS;

    /**
     * 故障转移的左移的位数 20 位
     */
    private final static int PROCESS_LEFT_SHIFT = WORKER_ID_LEFT_SHIFT + WORK_ID_BITS;

    /**
     * 时间戳左移的位数 22 位
     */
    private final static int TIMESTAMP_LEFT_SHIFT = PROCESS_LEFT_SHIFT + PROCESS_ID_BITS;

    /**
     * sequence 的掩码，确保 sequence 不会超出上限
     * 4095 = 111111111111
     * 任何打大于 4095 的数 & 上这个数都会重新变为小于等于 4095
     */
    private final static long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    /**
     * 进程 id 的掩码, 确保 process id 不会超出上限
     * 3 = 11
     * 任何大于 3 的数 & 上这个数都会重新变为小于等于 3
     */
    private static long PROCESS_MASK = ~(-1L << PROCESS_ID_BITS);

    /**
     * 上一次使用的时间戳
     */
    private volatile long lastTimestamp = -1L;

    /**
     * 同一个毫秒内, 递增的序列号
     */
    private volatile long sequence = 1L;

    /**
     * work id 分配器
     */
    private final WorkerIdAssigner workerIdAssigner;

    /**
     * 开始纪元的时间戳
     */
    private final long startEpochTimestamp;

    /**
     * 当前应用的进程 id, 占 2 位, 也就是 0-3
     */
    private final long processId;

    public SnowFlakeIdGenerator(WorkerIdAssigner workerIdAssigner, long startEpochTimestamp) {

        Objects.requireNonNull(workerIdAssigner);
        if (startEpochTimestamp >= System.currentTimeMillis()) {
            throw new IllegalArgumentException("[SnowFlakeId] the startEpochTimestamp can't be greater than current time");
        }
        this.workerIdAssigner = workerIdAssigner;
        this.startEpochTimestamp = startEpochTimestamp;
        this.processId = getProcessId();
    }

    @Override
    public synchronized Long generate() {

        long currentTime = System.currentTimeMillis();
        if (currentTime < this.lastTimestamp) {
            // 进行异常提示, 然后继续按照新的时间进行处理
            try {
                throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %s milliseconds", lastTimestamp - currentTime));
            } catch (Exception e) {
                logger.error("[SnowFlakeId] generate id fail", e);
            }
        }

        // 如果时间戳与上次时间戳相同
        if (currentTime == this.lastTimestamp) {
            // 序列号 加 1, 同时 & 上 4095, 确保序列号不会大于 4095
            this.sequence = (this.sequence + 1L) & SEQUENCE_MASK;

            // 序列号等于 0, 当前毫秒内计数满了，则等待下一秒
            if (this.sequence == 0L) {
                currentTime = this.tillNextMillis(currentTime);
            }
        } else {
            // 当前的时间大于上一次生成雪花 id 的时间戳, 将自增序列号恢复到
            this.sequence = 0L;
        }

        this.lastTimestamp = currentTime;
        return this.sequence
                | getWorkId() << WORKER_ID_LEFT_SHIFT
                | this.processId << PROCESS_LEFT_SHIFT
                | (currentTime - this.startEpochTimestamp) << TIMESTAMP_LEFT_SHIFT;
    }

    @Override
    public void appClose() {
        // 应用关闭, 释放资源
        this.workerIdAssigner.appClose();
    }

    /**
     * 获取进程占 2 位的进程 id, 0 - 3 之间
     *
     * @return 占 2 位的进程 id
     */
    private long getProcessId() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        long processId = Long.parseLong(runtimeMXBean.getName().split(Constants.PID_DELIMITER)[0]);
        return processId & PROCESS_MASK;
    }

    /**
     * 获取 work id
     *
     * @return 需要的 work id
     */
    private long getWorkId() {
        int workId = workerIdAssigner.workIdAssigner();
        if (workId > Constants.MAX_WORK_ID || workId < 0) {
            throw new IllegalArgumentException(String.format("[SnowflakeId] worker id can't be greater than %d or less than 0", Constants.MAX_WORK_ID));
        }
        return workId;
    }

    /**
     * 获取比入参大的时间戳
     *
     * @param compareTimestamp 比较的时间戳
     * @return 比入参大的时间戳
     */
    private long tillNextMillis(long compareTimestamp) {

        long timestamp = System.currentTimeMillis();
        while (timestamp <= compareTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
