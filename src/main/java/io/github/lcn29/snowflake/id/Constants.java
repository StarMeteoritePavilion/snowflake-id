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
     * 机房号: 0 - 4
     */
    public static final int MAX_COMPUTER_ROOM_ID = 1 << 2;

    /**
     * work id: 0 - 256
     */
    public static final int MAX_WORK_ID = 1 << 8;

    public static void main(String[] args) {
        System.out.println(MAX_WORK_ID);
        System.out.println(MAX_COMPUTER_ROOM_ID);
    }
}
