package simpledb;

import simpledb.dbfile.DBHeapFile;
import simpledb.exception.DBException;
import simpledb.matadata.table.TableDesc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class Catalog {

    private Map<String, TableInfo> tableMap;

    public Catalog() {
        tableMap = new ConcurrentHashMap<>();
    }

    public void addTable(String tableId, TableDesc tableDesc, DBHeapFile dbHeapFile){
        tableMap.put(tableId,new TableInfo(tableDesc,dbHeapFile,tableId));
    }

    public TableInfo getTableById(String tableId){
        TableInfo tableInfo = tableMap.get(tableId);
        if(tableInfo != null){
            return tableInfo;
        }else{
            throw new DBException("not find tableInfo: tableId=" + tableId);
        }
    }


    public class TableInfo{
        private TableDesc tableDesc;
        private DBHeapFile dbHeapFile;
        private String tableId;

        public TableInfo(TableDesc tableDesc, DBHeapFile dbHeapFile, String tableId) {
            this.tableDesc = tableDesc;
            this.dbHeapFile = dbHeapFile;
            this.tableId = tableId;
        }

        public TableDesc getTableDesc() {
            return tableDesc;
        }

        public DBHeapFile getDbHeapFile() {
            return dbHeapFile;
        }

        public String getTableId() {
            return tableId;
        }
    }
}
