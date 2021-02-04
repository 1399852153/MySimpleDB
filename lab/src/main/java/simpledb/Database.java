package simpledb;

/**
 * @author xiongyx
 * @date 2021/2/4
 */
public class Database {

    private static BufferPool bufferPool;
    private static final int DEFAULT_PAGE_CAPACITY = 500;

    public Database() {
        bufferPool = new BufferPool(DEFAULT_PAGE_CAPACITY);
    }

    public static BufferPool getBufferPool() {
        return bufferPool;
    }
}
