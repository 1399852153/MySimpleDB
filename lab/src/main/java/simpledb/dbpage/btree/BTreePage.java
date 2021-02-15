package simpledb.dbpage.btree;

import simpledb.dbpage.DBPage;
import simpledb.exception.DBException;
import simpledb.matadata.table.TableDesc;

/**
 * @author xiongyx
 * @date 2021/2/15
 */
public abstract class BTreePage implements DBPage {

    protected final TableDesc tableDesc;
    protected final BTreePageId pageId;
    protected int parent; // parent is always internal node or 0 for root node

    public BTreePage(TableDesc tableDesc,BTreePageId pageId) {
        this.tableDesc = tableDesc;
        this.pageId = pageId;
    }

    public BTreePageId getParentId() {
        if(parent == 0) {
            return BTreeRootPtrPage.getId(this.tableDesc.getTableId());
        }else{
            return new BTreePageId(this.tableDesc.getTableId(), parent, BTreePageCategoryEnum.INTERNAL.getValue());
        }
    }

    public BTreePageId getBTreePageId(){
        return this.pageId;
    }

    /**
     * Set the parent id
     * @param id - the id of the parent of this page
     */
    public void setParentId(BTreePageId id){
        if(id == null) {
            throw new DBException("parent id must not be null");
        }
        if(!id.getTableId().equals(this.tableDesc.getTableId())) {
            throw new DBException("table id mismatch in setParentId");
        }
        if(id.getPageCategory() != BTreePageCategoryEnum.INTERNAL.getValue()
                && id.getPageCategory() != BTreePageCategoryEnum.ROOT_PTR.getValue()) {
            throw new DBException("parent must be an internal node or root pointer");
        }
        if(id.getPageCategory() == BTreePageCategoryEnum.ROOT_PTR.getValue()) {
            parent = 0;
        }
        else {
            parent = id.getPageNo();
        }
    }
}
