package simpledb;

/**
 * @author xiongyx
 * @date 2021/2/4
 */
public class Database {

    private static final BufferPool bufferPool;
    private static final Category category;
    private static final int DEFAULT_PAGE_CAPACITY = 500;

    static{
        bufferPool = new BufferPool(DEFAULT_PAGE_CAPACITY);
        category = new Category();
    }

    public static Category getCategory(){
        return category;
    }

    public static BufferPool getBufferPool() {
        return bufferPool;
    }
}
