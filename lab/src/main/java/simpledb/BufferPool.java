package simpledb;

/**
 * @author xiongyx
 * @date 2021/2/3
 */
public class BufferPool {

    private static final int DEFAULT_PAGE_SIZE = 4 * 1024;


    public static int getPageSize(){
        return DEFAULT_PAGE_SIZE;
    }
}
