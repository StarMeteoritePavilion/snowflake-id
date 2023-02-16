package io.github.lcn29.snowflake.id;

/**
 * <pre>
 *
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-15 23:13
 */
public class Constants {

    /**
     * 机房号: 0 - 4
     */
    public static final int MAX_COMPUTER_ROOM_ID = 1 << 2;

    /**
     * work id: 0 - 256
     */
    public static final int MAX_WORK_ID = 1 << 8;

    /**
     * ZooKeeper 连接超时时间
     */
    public static final int CONNECTION_TIMEOUT_MS = 5000;

    /**
     * ZooKeeper 会话超时时间
     */
    public static final int SESSION_TIMEOUT_MS = 5000;

    /**
     * 重连机制, 2 次重连的时间间隔
     */
    public static final int BASE_SLEEP_TIME_MS = 1000;

    /**
     * 重试机制, 最多的尝试次数
     */
    public static final int MAX_RETRIES = 3;

    /**
     * 获取 ZooKeeper 分布式锁的超时时间
     */
    public static final long GET_LOCK_TIMEOUT = 5000L;

    /**
     * 路径切割符
     */
    public static final String DELIMITER = "/";

    /**
     * 存储在 ZooKeeper 节点的数据格式
     */
    public static final String ZOOKEEPER_NODE_DATA_FORMAT = "[ip:%s,host:%s,pid:%s]";

    /**
     * pid 的分割符
     */
    public static final String pid_delimiter = "@";
}
