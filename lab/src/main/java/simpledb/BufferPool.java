package simpledb;

import simpledb.dbpage.DBHeapPage;
import simpledb.dbpage.PageId;
import simpledb.exception.DBException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xiongyx
 * @date 2021/2/3
 */
public class BufferPool {

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private ConcurrentHashMap<PageId, DBHeapPage> pageCacheMap = new ConcurrentHashMap<>();

    private int maxPageSize;
    private int pageCapacity;

    public BufferPool(int maxPageSize, int pageCapacity) {
        this.maxPageSize = maxPageSize;
        this.pageCapacity = pageCapacity;
    }


    public BufferPool(int pageCapacity) {
        this.pageCapacity = pageCapacity;
    }

    public int getPageSize(){
        return DEFAULT_PAGE_SIZE;
    }

    public DBHeapPage getPage(PageId pageId){
        if (pageCacheMap.contains(pageId)) {
            return pageCacheMap.get(pageId);
        }else{
            if (pageCacheMap.size() < this.maxPageSize) {
//                DBHeapPage page =
//                        Database.getCatalog()
//                                .getDatabaseFile(pid.getTableId())
//                                .readPage(pid);
                // 加入缓存
//                pageCacheMap.put(pageId, page);
//                return page;

                return null;
            } else {
                // todo 使用淘汰算法置换页面
                throw new DBException("BufferPool is full");
            }
        }
    }
}
