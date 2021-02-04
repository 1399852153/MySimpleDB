package simpledb;

/**
 * @author xiongyx
 * @date 2021/2/3
 */
public class BufferPool {

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private int pageCapacity;

    public BufferPool(int pageCapacity) {
        this.pageCapacity = pageCapacity;
    }

    public int getPageSize(){
        return DEFAULT_PAGE_SIZE;
    }
}
