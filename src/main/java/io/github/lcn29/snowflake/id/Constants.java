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
     * work id 位数, 8 位
     */
    private final static int WORK_ID_BITS = 8;

    /**
     * work id: 0 - 256
     */
    public static final int MAX_WORK_ID = 1 << WORK_ID_BITS;

    /**
     * pid 的分割符
     */
    public static final String PID_DELIMITER = "@";
}
