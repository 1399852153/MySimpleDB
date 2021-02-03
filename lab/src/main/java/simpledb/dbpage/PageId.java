package simpledb.dbpage;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class PageId {

    /**
     * 表示当前页是属于那张表的
     * */
    private String tableId;

    /**
     * 对应表文件的第几页
     * */
    private int pageNo;

    public PageId(String tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    public String getTableId() {
        return tableId;
    }

    public int getPageNo() {
        return pageNo;
    }
}
