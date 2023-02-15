package io.github.lcn29.snowflake.id.assigner;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * 当前的 workId
     */
    private int curWorkId = -1;

    /**
     * 应用的 appName
     */
    private String appName;

    /**
     * ZooKeeper 的地址
     */
    private String zooKeeperUrl;

    /**
     * ZooKeeper 的端口
     */
    private int zooKeeperPort;

    /**
     * ZooKeeper 连接客户端
     */
    private CuratorFramework curatorFramework;

    public DefaultWorkIdAssigner(String appName, String zooKeeperUrl, int zooKeeperPort) {
        this.appName = appName;
        this.zooKeeperUrl = zooKeeperUrl;
        this.zooKeeperPort = zooKeeperPort;
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


    private void init() {
        if (this.appName == null || this.appName.trim().length() == 0) {
            throw new RuntimeException("【SnowFlakeId】appName can not be empty !");
        }

        if (this.curatorFramework != null) {
            this.curatorFramework.close();
        }

    }
}
