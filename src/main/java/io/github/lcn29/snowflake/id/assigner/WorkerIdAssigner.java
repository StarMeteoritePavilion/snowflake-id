package io.github.lcn29.snowflake.id.assigner;

/**
 * <pre>
 * work id 分配器
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-15 23:10
 */
public interface WorkerIdAssigner {

    /**
     * 分配 workId
     *
     * @return 需要的 workId
     */
    int workIdAssigner();

    /**
     * 应用关闭回调
     */
    default void appClose() {
    }

}
