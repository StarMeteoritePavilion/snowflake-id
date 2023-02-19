package io.github.lcn29.snowflake.id;

import io.github.lcn29.snowflake.id.assigner.WorkerIdAssigner;
import io.github.lcn29.snowflake.id.assigner.ZooKeeperWorkIdAssigner;
import io.github.lcn29.snowflake.id.generator.IdGenerator;
import io.github.lcn29.snowflake.id.generator.SnowFlakeIdGenerator;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * <pre>
 *
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-19 18:59
 */
public class IdGeneratorTest {

    // ZooKeeper 的地址
    private static String zooKeeperUrl = "sr-test-zk-cluster-1.gz.cvte.cn";

    // ZooKeeper 的端口
    private static int zooKeeperPort = 2181;

    // 应用名称
    private static String appName = "lcn-test";

    // 2023-02-01 00:00:00 时间戳
    private static long startEpochTimestamp = 1675180800000L;

    // 创建的线程数
    private static int createThreadNum = 1000;
    // 每个线程获取的 id 数
    private static int getIdCountEveryThread = 10000;
    // 总共需要获取的 id 数
    private static int totalIdNum = createThreadNum * getIdCountEveryThread;

    @Test
    public void test() {

        WorkerIdAssigner zooKeeperWorkIdAssigner = new ZooKeeperWorkIdAssigner(appName, zooKeeperUrl, zooKeeperPort);
        IdGenerator idGenerator = new SnowFlakeIdGenerator(zooKeeperWorkIdAssigner, startEpochTimestamp);

        Map<Long, Integer> idCountMap = new ConcurrentHashMap<>(totalIdNum);

        long startTime = System.currentTimeMillis();

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch endGate = new CountDownLatch(createThreadNum);

        Runnable task = createTask(startGate, endGate, idGenerator, idCountMap);

        for (int i = 0; i < createThreadNum; i++) {
            // 启动线程
            new Thread(task, "CanTest-" + i).start();
        }

        System.out.println("线程创建完成, 准备启动");

        // 减 1, 所有线程启动
        startGate.countDown();


        try {
            // 等待, 直到 endGate 为 0 (初始值为所有线程数量, 为 0, 所有子线程执行完成)
            endGate.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

        idGenerator.appClose();

        // 输出次数大于 2 的 id
        idCountMap.forEach((k, v) -> {
            if (v >= 2) {
                System.out.println("id:" + k + ", 出现了 2 次以上, 次数:" + v);
            }
        });

        System.out.println("cost: " + (System.currentTimeMillis() - startTime) + ", id count: " + idCountMap.size());
    }

    /**
     * 创建线程任务
     *
     * @param startGate   阻塞 CountDownLatch, 变为 0, 子线程启动
     * @param endGate     倒计时 CountDownLatch, 减为 0, 主线程启动
     * @param idGenerator id 生成器
     * @param idCountMap  生成的 id 存放处
     * @return
     */
    private Runnable createTask(CountDownLatch startGate, CountDownLatch endGate, IdGenerator idGenerator, Map<Long, Integer> idCountMap) {
        return () -> {
            try {
                // 所有线程等待, 指定 startGate 变为 0, (初始为 1)
                startGate.await();
                for (int i = 0; i < getIdCountEveryThread; i++) {
                    Long id = idGenerator.generate();
                    idCountMap.put(id, (idCountMap.getOrDefault(id, 0) + 1));
                    //System.out.println(i + "---" + id);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // 线程执行完成, endGate 减 1
                endGate.countDown();
            }
        };
    }


}
