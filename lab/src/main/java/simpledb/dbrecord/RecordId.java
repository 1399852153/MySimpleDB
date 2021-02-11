package simpledb.dbrecord;

import simpledb.dbpage.PageId;
import simpledb.dbpage.normal.HeapPageId;

import java.util.Objects;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class RecordId {
    /**
     * 当前记录是属于哪一页的
     * */
    private PageId pageId;

    /**
     * 当前记录在页内的下标记录号
     * */
    private int pageInnerNo;

    public RecordId(PageId pageId, int pageInnerNo) {
        this.pageId = pageId;
        this.pageInnerNo = pageInnerNo;
    }

    public int getPageInnerNo() {
        return pageInnerNo;
    }

    public PageId getPageId() {
        return pageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordId recordId = (RecordId) o;
        return pageInnerNo == recordId.pageInnerNo && pageId.equals(recordId.pageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, pageInnerNo);
    }

    @Override
    public String toString() {
        return "RecordId{" +
                "pageId=" + pageId +
                ", pageInnerNo=" + pageInnerNo +
                '}';
    }
}
