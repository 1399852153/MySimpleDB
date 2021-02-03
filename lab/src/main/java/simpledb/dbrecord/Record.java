package simpledb.dbrecord;

import simpledb.matadata.fields.Field;
import simpledb.matadata.table.TableDesc;

import java.util.List;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class Record {
    private RecordId recordId;

    private TableDesc tableDesc;
    private List<Field> fieldList;


    public TableDesc getTableDesc() {
        return tableDesc;
    }

    public void setTableDesc(TableDesc tableDesc) {
        this.tableDesc = tableDesc;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }

    public List<Field> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<Field> fieldList) {
        this.fieldList = fieldList;
    }
}
