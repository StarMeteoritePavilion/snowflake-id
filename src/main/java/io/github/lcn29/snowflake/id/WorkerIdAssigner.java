package io.github.lcn29.snowflake.id;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <pre>
 *
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-12 22:01
 */
public class WorkerIdAssigner {

    private static final Logger log = LoggerFactory.getLogger(WorkerIdAssigner.class);

    private static final String ROOT_NAME = "/workId";

    private static final String NODE_NAME = "/lockId";

    private String appName;

    private String zookeerUrl;

    private Integer zookeerPort;

    private static Set<Integer> OrderIdSet = new HashSet();

    private int workerId = -1;

    private CuratorFramework curatorFramework;

    static {
        // 8 个 2
        for(int i = 0; i < 256; ++i) {
            OrderIdSet.add(i);
        }
    }


    public WorkerIdAssigner(String appName, String zookeerUrl, Integer zookeerPort) {
        this.appName = appName;
        this.zookeerUrl = zookeerUrl;
        this.zookeerPort = zookeerPort;
    }

    public void close() {
        if (this.curatorFramework != null) {
            curatorFramework.close();
        }
    }

    private CuratorFramework initCuratorFramework() {
        String zookeeperConnectionString = this.zookeerUrl + ":" + this.zookeerPort;
        this.curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zookeeperConnectionString)
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.curatorFramework.start();
        return this.curatorFramework;
    }

    private void buildWorkId() {

        if (this.curatorFramework == null) {
            throw new RuntimeException("ZooKeeper Client not Found");
        }

        String curAppBasePath = ROOT_NAME + "/" + this.appName;
        String nodePath = ROOT_NAME + "/" + this.appName + NODE_NAME;

        InterProcessMutex interProcessLock = new InterProcessMutex(this.curatorFramework, curAppBasePath);

        try {

            if (!interProcessLock.acquire(5000L, TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("ZK分布式锁 加锁超时，超时时间: 5000");
            }

            Stat stat = this.curatorFramework.checkExists().forPath(nodePath);
            if (stat == null) {
                this.curatorFramework.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath);
            }

            List<String> childPath = this.curatorFramework.getChildren().forPath(nodePath);
            Set<Integer> nodeIdSet = new LinkedHashSet<>();
            if (childPath == null || childPath.isEmpty()) {
                Iterator<String> iterator = childPath.iterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    try {
                        nodeIdSet.add(Integer.valueOf(next));
                    } catch (Exception var26) {
                        //log.warn("路径由不合法操作创建，注意[" + lockPath + "]仅用于构建workId");
                    }
                }

            }

            Iterator<Integer> iterator = OrderIdSet.iterator();

            while (iterator.hasNext()) {
                Integer next = iterator.next();
                if (!nodeIdSet.contains(next)) {
                    final String currentNodePath = nodePath + "/" + next;
                    String nodeDate = String.format("[ip:%s,hostname:%s,pid:%s]", InetAddress.getLocalHost().getHostAddress(), InetAddress.getLocalHost().getHostName(), ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

                    try {
                        this.curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(currentNodePath, nodeDate.getBytes("UTF-8"));
                    } catch (Exception var27) {
                        log.debug("节点[{}]无法创建，可能是已存在", currentNodePath);
                        continue;
                    }


                    final long pathCreateTime = ((Stat)this.curatorFramework.checkExists().forPath(currentNodePath)).getCtime();
                    final TreeCache treeCache = new TreeCache(this.curatorFramework, currentNodePath);

                    treeCache.getListenable().addListener(new TreeCacheListener() {
                        @Override
                        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                            long pathTime;
                            try {
                                pathTime = ((Stat)curatorFramework.checkExists().forPath(currentNodePath)).getCtime();
                            } catch (Exception var12) {
                                pathTime = 0L;
                            }
                            if (pathCreateTime != pathTime) {
                                //DefaultWorkerIdAssigner.log.info("从ZK断开，再次注册...");

                                try {
                                    treeCache.close();
                                } catch (Exception var10) {
                                   // DefaultWorkerIdAssigner.log.warn("treeCache关闭失败");
                                } finally {
                                  //  DefaultWorkerIdAssigner.this.buildWorkId(appPath);
                                }
                            }
                        }
                    });

                    treeCache.start();
                    this.workerId = next;
                    log.info("基于ZK成功构建 workId:{}", this.workerId);
                    return;
                }
            }


        } catch (Exception exception) {
            // TODO
        } finally {
            try {
                interProcessLock.release();
            } catch (Exception exception) {
                // TODO
            }

            if (this.curatorFramework != null) {
                this.curatorFramework.close();
            }

        }
    }


}
