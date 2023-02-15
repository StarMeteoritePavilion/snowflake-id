package io.github.lcn29.snowflake.id.generator;

import java.util.List;

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
     * 批量产生多个雪花 id
     *
     * @param size 需要的雪花 id 个数
     * @return 雪花 id 列表
     */
    List<Long> batchGenerate(int size);
}
