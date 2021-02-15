package simpledb;

import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbpage.normal.HeapPageId;
import simpledb.exception.DBException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xiongyx
 * @date 2021/2/3
 */
public class BufferPool {

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private final ConcurrentHashMap<PageId, DBPage> pageCacheMap = new ConcurrentHashMap<>();

    private int maxPageSize;

    public BufferPool(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }


    public int getPageSize(){
        return DEFAULT_PAGE_SIZE;
    }

    public DBPage getPage(PageId pageId){
        if (pageCacheMap.contains(pageId)) {
            return pageCacheMap.get(pageId);
        }else{
            if (pageCacheMap.size() < this.maxPageSize) {
                DBPage page = Database.getCatalog()
                                .getTableById(pageId.getTableId())
                                .getDbFile()
                                .readPage(pageId);
                // 加入缓存
                pageCacheMap.put(pageId, page);
                return page;
            } else {
                // todo 使用淘汰算法置换页面
                throw new DBException("BufferPool is full");
            }
        }
    }
}
