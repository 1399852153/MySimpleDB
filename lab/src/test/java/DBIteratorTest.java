import org.junit.Assert;
import org.junit.Test;
import simpledb.Database;
import simpledb.dbfile.DBFile;
import simpledb.dbfile.DBHeapFile;
import simpledb.dbpage.DBHeapPage;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.dbrecord.RecordId;
import simpledb.iterator.DbFileIterator;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.fields.StringField;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.File;
import java.util.Arrays;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public class DBIteratorTest {

    @Test
    public void testDBFileIterator(){
        String tableId = "people";
        TableDesc tableDesc = new TableDesc(
                "people",
                Arrays.asList(
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.STRING_TYPE
                )
        );

        File file = new File("table1");
        DBFile table1File = new DBHeapFile(tableDesc,file);
        Database.getCatalog().addTable(tableId,tableDesc,table1File);

        int pageNum = 3;
        for(int j=0; j<pageNum; j++) {
            PageId pageId = new PageId(tableId,j);
            // 测试插入、删除
            DBPage dbHeapPage = new DBHeapPage(tableDesc,pageId,new byte[Database.getBufferPool().getPageSize()]);
            Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),0);

            int maxSlot = dbHeapPage.getMaxSlotNum();
            for (int i = 0; i < maxSlot; i++) {
                Record record = new Record();
                record.setRecordId(new RecordId(pageId, i));
                record.setTableDesc(tableDesc);
                record.setFieldList(Arrays.asList(
                        new IntField(i),
                        new IntField(j),
                        new StringField("a" + i + "-b" + j))
                );
                dbHeapPage.insertRecord(record);
            }

            table1File.writePage(dbHeapPage);
        }

        DbFileIterator<Record> dbFileIterator = table1File.getIterator();
        Assert.assertFalse(dbFileIterator.hasNext());

        dbFileIterator.open();
        Assert.assertTrue(dbFileIterator.hasNext());

        while(dbFileIterator.hasNext()){
            Record record = dbFileIterator.next();
            System.out.println(record);
        }
        Assert.assertFalse(dbFileIterator.hasNext());
        dbFileIterator.reset();
        Assert.assertTrue(dbFileIterator.hasNext());
    }
}
