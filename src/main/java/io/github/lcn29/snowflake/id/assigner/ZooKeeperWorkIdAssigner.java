package io.github.lcn29.snowflake.id.assigner;

import io.github.lcn29.snowflake.id.Constants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <pre>
 * 默认的 work id 分配器
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-15 23:12
 */
public class ZooKeeperWorkIdAssigner implements WorkerIdAssigner {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperWorkIdAssigner.class);

    /**
     * 路径分隔符
     */
    private static final String DELIMITER = "/";

    /**
     * 冒号
     */
    private static final String COLON = ":";

    /**
     * ZooKeeper 连接超时时间
     */
    private static final int CONNECTION_TIMEOUT_MS = 5000;

    /**
     * ZooKeeper 会话超时时间
     */
    private static final int SESSION_TIMEOUT_MS = 5000;

    /**
     * 重连机制, 2 次重连的时间间隔
     */
    private static final int BASE_SLEEP_TIME_MS = 1000;

    /**
     * 重试机制, 最多的尝试次数
     */
    private static final int MAX_RETRIES = 3;

    /**
     * 获取 ZooKeeper 分布式锁的超时时间
     */
    private static final long GET_LOCK_TIMEOUT = 5000L;

    /**
     * 存储在 ZooKeeper 节点的数据格式
     */
    private static final String ZOOKEEPER_NODE_DATA_FORMAT = "[ip:%s,host:%s,pid:%s]";

    /**
     * workId 注册的节点根路径
     */
    private static final String ROOT_PATH = "/snowflake/workId";

    /**
     * 路径锁标识, 用来 ZooKeeper 分布式锁
     */
    private static final String LOCK_PATH = "/lockId";

    /**
     * 应用的 appName
     */
    private final String appName;

    /**
     * ZooKeeper 连接地址字符串, 地址:端口
     */
    private final String zooKeeperConnectString;

    /**
     * ZooKeeper 连接客户端
     */
    private CuratorFramework curatorFramework;

    /**
     * ZooKeeper 注册的 worker id 节点的监听器
     */
    private TreeCache treeCache;

    /**
     * 当前的 workId
     */
    private volatile int curWorkId;

    public ZooKeeperWorkIdAssigner(String appName, String zooKeeperUrl, int zooKeeperPort) {
        this.appName = appName;
        this.zooKeeperConnectString = zooKeeperUrl + COLON + zooKeeperPort;
        // 创建客户端
        this.createCuratorFramework();
        // 注册路径 + 分配 work id
        this.allocateWorkId();
    }

    @Override
    public int workIdAssigner() {
        return this.curWorkId;
    }

    @Override
    public void appClose() {
        Optional.ofNullable(this.treeCache).ifPresent(TreeCache::close);
        Optional.ofNullable(this.curatorFramework).ifPresent(CuratorFramework::close);
    }

    /**
     * 创建与 ZooKeeper 连接的客户端 CuratorFramework
     */
    private void createCuratorFramework() {
        if (this.appName == null || this.appName.trim().length() == 0) {
            throw new RuntimeException("[SnowFlakeId]appName can not be empty !");
        }

        if (this.curatorFramework != null) {
            this.curatorFramework.close();
        }

        // 创建 ZooKeeper 客户端
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zooKeeperConnectString)
                .connectionTimeoutMs(CONNECTION_TIMEOUT_MS)
                .sessionTimeoutMs(SESSION_TIMEOUT_MS)
                // 设置重试机制
                .retryPolicy(new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES))
                .build();
        // 启动
        this.curatorFramework.start();
    }

    /**
     * 分配 work id
     */
    private void allocateWorkId() {
        if (this.curatorFramework == null) {
            throw new RuntimeException("[SnowFlakeId]ZooKeeper client not found");
        }

        if (this.curatorFramework.getState() == CuratorFrameworkState.STOPPED) {
            this.curatorFramework.start();
        }

        String curAppPath = ROOT_PATH + DELIMITER + this.appName;
        String curAppLockPath = curAppPath + LOCK_PATH;

        InterProcessMutex interProcessMutex = new InterProcessMutex(this.curatorFramework, curAppPath);

        try {
            // interProcessMutex 声明的路径会在获取锁时创建出来
            if (!interProcessMutex.acquire(GET_LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("[SnowFlakeId]acquire ZooKeeper distributed lock time");
            }
            Stat curAppLockPathStat = this.curatorFramework.checkExists().forPath(curAppLockPath);
            if (curAppLockPathStat == null) {
                // 路径不存在进行创建, 非持久的
                this.curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(curAppLockPath);
            }

            // 当前在 curAppLockPath 上的 work id 列表
            Set<Integer> curWorkIds = new LinkedHashSet<>();
            List<String> childPath = this.curatorFramework.getChildren().forPath(curAppLockPath);

            if (childPath != null && !childPath.isEmpty()) {
                // childPath 有数据
                childPath.forEach(item -> {
                    try {
                        curWorkIds.add(Integer.valueOf(item));
                    } catch (NumberFormatException e) {
                        logger.error("[SnowFlakeId]the work id in ZooKeeper format error");
                    }
                });
            }

            for (int index = 0; index <= Constants.MAX_WORK_ID; index++) {
                // 当前的 index 对应的 work id 已经被分配了
                if (curWorkIds.contains(index)) {
                    continue;
                }

                // 最终维护在 ZooKeeper 的路径
                String curNodePath = curAppLockPath + DELIMITER + index;
                String nodeData = String.format(
                        ZOOKEEPER_NODE_DATA_FORMAT,
                        InetAddress.getLocalHost().getHostAddress(),
                        InetAddress.getLocalHost().getHostName(),
                        ManagementFactory.getRuntimeMXBean().getName().split(Constants.PID_DELIMITER)[0]
                );

                try {
                    // 创建节点
                    this.curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(curNodePath, nodeData.getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    logger.error("[SnowFlakeId] Node:[{}] creat fail, maybe exist", curNodePath);
                }

                this.treeCache = createTreeCache(curAppPath);
                this.treeCache.start();
                this.curWorkId = index;
                logger.info("[SnowFlakeId] ZooKeeper connect success, the workId is :{}", index);
                return;
            }

        } catch (Exception e) {
            logger.error("[SnowFlakeId]allocation work id by ZooKeeper fail: ", e);
            throw new RuntimeException("[SnowFlakeId]allocation work id by ZooKeeper fail");
        } finally {
            // 锁释放
            interProcessMutexRelease(interProcessMutex);
        }
    }

    /**
     * 创建 TreeCache
     *
     * @param watchPath 需要监听的路径
     * @return TreeCache 对象
     * @throws Exception
     */
    private TreeCache createTreeCache(String watchPath) throws Exception {

        TreeCache treeCache = new TreeCache(this.curatorFramework, watchPath);
        long watchPathCreatTime = this.curatorFramework.checkExists().forPath(watchPath).getCtime();

        // 建立监听器, 在 ZooKeeper 节点有变更时, 进行更新
        treeCache.getListenable().addListener((client, treeCacheEvent) -> {
            long pathCreatTime;

            try {
                // 获取当前监听节点的创建时间
                pathCreatTime = this.curatorFramework.checkExists().forPath(watchPath).getCtime();
            } catch (Exception e) {
                logger.error("[SnowFlackId]the tree cache listener exception", e);
                pathCreatTime = 0;
            }
            // pathCreatTime 改变了, 那么只能一种情况:
            // 当前应用与 zk 失去联系, 且 {curNodePath} 不存在或者被其它应用占据了 (表象为 pathCreatTime 变化)
            // 无论哪种情况,当前应用都要重新注册节点
            if (pathCreatTime != watchPathCreatTime) {
                logger.info("[SnowFlakeId] disconnect with ZooKeeper, try reconnection ...");
                try {
                    treeCache.close();
                } catch (Exception e) {
                    logger.error("[SnowFlakeId] TreeCache close fail");
                }
                // 重新分配
                allocateWorkId();
            }
        });

        return treeCache;
    }

    /**
     * ZooKeeper 分布式锁释放
     *
     * @param interProcessMutex 分布式锁
     */
    private void interProcessMutexRelease(InterProcessMutex interProcessMutex) {
        try {
            interProcessMutex.release();
        } catch (Exception e) {
            logger.error("[SnowFlakeId]distributed ZooKeeper lock release fail: ", e);
            throw new RuntimeException("[SnowFlakeId]distributed ZooKeeper Lock release fail");
        }
    }
    
}
