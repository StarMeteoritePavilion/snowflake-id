package io.github.lcn29.snowflake.id.assigner;

import io.github.lcn29.snowflake.id.Constants;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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
public class DefaultWorkIdAssigner implements WorkerIdAssigner {

    private static final Logger logger = LoggerFactory.getLogger(DefaultWorkIdAssigner.class);

    /**
     * 根路径
     */
    private static final String ROOT_PATH = "/snowflake/workId";

    /**
     * 路径锁标识, 用来 ZooKeeper 分布式锁
     */
    private static final String LOCK_PATH = "/lockId";

    /**
     * 当前的 workId
     */
    private int curWorkId;

    /**
     * 应用的 appName
     */
    private final String appName;

    /**
     * ZooKeeper 的地址
     */
    private final String zooKeeperUrl;

    /**
     * ZooKeeper 的端口
     */
    private final int zooKeeperPort;

    /**
     * ZooKeeper 连接客户端
     */
    private CuratorFramework curatorFramework;

    public DefaultWorkIdAssigner(String appName, String zooKeeperUrl, int zooKeeperPort) {
        this.appName = appName;
        this.zooKeeperUrl = zooKeeperUrl;
        this.zooKeeperPort = zooKeeperPort;
        this.init();
    }

    @Override
    public int workIdAssigner() {
        return this.curWorkId;
    }

    @Override
    public void appClose() {
        // 应用关闭时, 关闭客户端连接
        if (curatorFramework != null) {
            this.curatorFramework.close();
        }
    }

    /**
     * WorkIdAssigner 初始化
     */
    private void init() {

        if (this.appName == null || this.appName.trim().length() == 0) {
            throw new RuntimeException("[SnowFlakeId]appName can not be empty !");
        }

        if (this.curatorFramework != null) {
            this.curatorFramework.close();
        }
        // ZooKeeper 连接的字符串, 地址:端口
        String zooKeeperConnectString = this.zooKeeperUrl + ":" + this.zooKeeperPort;

        // 创建 ZooKeeper 客户端
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zooKeeperConnectString)
                .connectionTimeoutMs(Constants.CONNECTION_TIMEOUT_MS)
                .sessionTimeoutMs(Constants.SESSION_TIMEOUT_MS)
                // 设置重试机制
                .retryPolicy(new ExponentialBackoffRetry(Constants.BASE_SLEEP_TIME_MS, Constants.MAX_RETRIES))
                .build();
        // 启动应用
        this.curatorFramework.start();
        // 开始分配 work id
        this.allocationWorkId();
    }

    /**
     * 分配 work id
     */
    private void allocationWorkId() {
        if (this.curatorFramework == null) {
            throw new RuntimeException("[SnowFlakeId]ZooKeeper client not found");
        }

        String curAppPath = ROOT_PATH + Constants.DELIMITER + this.appName;
        String curAppLockPath = curAppPath + LOCK_PATH;

        InterProcessMutex interProcessMutex = new InterProcessMutex(this.curatorFramework, curAppPath);

        try {
            // interProcessMutex 声明的路径会在获取锁时创建出来
            if (!interProcessMutex.acquire(Constants.GET_LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
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
                String curNodePath = curAppLockPath + Constants.DELIMITER + index;
                String nodeData = String.format(
                        Constants.ZOOKEEPER_NODE_DATA_FORMAT,
                        InetAddress.getLocalHost().getHostAddress(),
                        InetAddress.getLocalHost().getHostName(),
                        ManagementFactory.getRuntimeMXBean().getName().split("@")[0]
                );

                try {
                    this.curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(curNodePath, nodeData.getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    logger.error("[SnowFlakeId] Node:[{}] creat fail, maybe exist", curNodePath);
                }

                long curNodePathCreateTime = this.curatorFramework.checkExists().forPath(curNodePath).getCtime();
                TreeCache treeCache = new TreeCache(this.curatorFramework, curNodePath);

                treeCache.getListenable().addListener((curatorFramework, treeCacheEvent) -> {
                    long pathCreatTime = 0;
                    try {
                        pathCreatTime = curatorFramework.checkExists().forPath(curNodePath).getCtime();
                    } catch (Exception e) {
                        pathCreatTime = 0;
                    }

                    if (pathCreatTime != curNodePathCreateTime) {
                        logger.info("[SnowFlakeId] disconnect with ZooKeeper, try reconnection ...");
                        try {
                            treeCache.close();
                        } catch (Exception e) {
                            logger.error("[SnowFlakeId] TreeCache close fail");
                        }
                        // 重新分配
                        allocationWorkId();
                    }
                });

                treeCache.start();
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
