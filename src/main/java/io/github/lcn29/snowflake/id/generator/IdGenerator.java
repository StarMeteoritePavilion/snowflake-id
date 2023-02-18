package io.github.lcn29.snowflake.id.generator;

/**
 * <pre>
 * 雪花 id 生产器
 * </pre>
 *
 * @author lcn29
 * @date 2023-02-15 23:07
 */
public interface IdGenerator {

    /**
     * 单独产生一个雪花 id
     *
     * @return 雪花 id
     */
    Long generate();

    /**
     * 应用关闭回调
     */
    default void appClose() {
    }
}
