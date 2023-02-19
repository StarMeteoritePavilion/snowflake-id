package io.github.lcn29.snowflake.id.assigner;

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

import javax.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <pre>
 *
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-19 16:43
 */
public class DefaultWorkerIdAssigner implements WorkerIdAssigner {
    private static final Logger log = LoggerFactory.getLogger(DefaultWorkerIdAssigner.class);
    private int workerId = -1;
    private CuratorFramework client;
    private static final String ROOT_NAME = "/workid";
    private static final String NODE_NAME = "/lockid";
    private String appName;
    private String url;
    private static final int MAX_ORDER = 256;
    private static Set<Integer> OrderIdSet = new HashSet();

    @Override
    public int workIdAssigner() {
        return this.workerId;
    }

    public DefaultWorkerIdAssigner(String appName, String url) {
        this.appName = appName;
        this.url = url;
        this.init();
    }

    @Override
    public void appClose() {
        if (null != this.client && null == this.client.getState()) {
            this.client.close();
        }

        this.client = null;
    }

    @PostConstruct
    public void init() {
        if (appName == null || appName.length() <= 0) {
            log.error("应用名称不能为空！");
            throw new RuntimeException("应用名称不能为空！");
        } else {
            if (this.client != null) {
                this.appClose();
            }

            this.client = CuratorFrameworkFactory.builder().connectString(this.url).connectionTimeoutMs(5000).sessionTimeoutMs(5000).canBeReadOnly(false).retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
            this.client.start();
            this.buildWorkId(this.appName);
        }
    }

    private void buildWorkId(final String appPath) {
        if (null == this.client) {
            throw new RuntimeException("本节点注册到ZK异常。");
        } else {
            String lockPath = "/workid" + "/" + this.appName;
            String nodePath = "/workid" + "/" + this.appName + "/lockid";
            InterProcessMutex interProcessLock = new InterProcessMutex(this.client, lockPath);

            try {
                if (!interProcessLock.acquire(5000L, TimeUnit.MILLISECONDS)) {
                    throw new TimeoutException("ZK分布式锁 加锁超时，超时时间: 5000");
                } else {
                    if (null == this.client.checkExists().forPath(nodePath)) {
                        this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath);
                    }

                    Iterator var7 = OrderIdSet.iterator();
                    Set<String > nodeIdSet =  new HashSet();


                    while(var7.hasNext()) {
                        Integer order = (Integer)var7.next();
                        if (!nodeIdSet.contains(order)) {
                            final String currentNodePath = nodePath + "/" + order;
                            String nodeDate = String.format("[ip:%s,hostname:%s,pid:%s]", InetAddress.getLocalHost().getHostAddress(), InetAddress.getLocalHost().getHostName(), ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

                            try {
                                this.client.create().withMode(CreateMode.EPHEMERAL).forPath(currentNodePath, nodeDate.getBytes("UTF-8"));
                            } catch (Exception var27) {
                                log.debug("节点[{}]无法创建，可能是已存在", currentNodePath);
                                continue;
                            }

                            final long pathCreateTime = ((Stat)this.client.checkExists().forPath(currentNodePath)).getCtime();
                            final TreeCache treeCache = new TreeCache(this.client, currentNodePath);
                            treeCache.getListenable().addListener(new TreeCacheListener() {

                                @Override
                                public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                                    long pathTime;
                                    try {
                                        pathTime = curatorFramework.checkExists().forPath(currentNodePath).getCtime();
                                    } catch (Exception var12) {
                                        pathTime = 0L;
                                    }

                                    if (pathCreateTime != pathTime) {
                                        log.info("从ZK断开，再次注册...");

                                        try {
                                            treeCache.close();
                                        } catch (Exception var10) {
                                            log.warn("treeCache关闭失败");
                                        } finally {
                                        }
                                    }

                                }
                            });
                            treeCache.start();
                            this.workerId = order;
                            log.info("基于ZK成功构建 workId:{}", this.workerId);
                            return;
                        }
                    }

                    throw new RuntimeException("获取WorkId失败，共[" + 256 + "]个可用WorkId, 已全部用完。 ");
                }
            } catch (Exception var28) {
                var28.printStackTrace();
                log.error("获取分布式WorkId异常", var28);
                throw new RuntimeException("获取WorkId失败，共[" + 256 + "]个可用WorkId, 已全部用完。 ");
            } finally {
                if (interProcessLock != null) {
                    try {
                        interProcessLock.release();
                    } catch (Exception var25) {
                        log.warn("释放锁失败");
                    }
                }

            }
        }
    }

    static {
        for(int i = 0; i < 256; ++i) {
            OrderIdSet.add(i);
        }

    }

}
