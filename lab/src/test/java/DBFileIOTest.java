import org.junit.Assert;
import org.junit.Test;
import simpledb.Database;
import simpledb.dbfile.DBFile;
import simpledb.dbfile.DBHeapFile;
import simpledb.dbpage.normal.DBHeapPage;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.normal.HeapPageId;
import simpledb.dbrecord.Record;
import simpledb.dbrecord.RecordId;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.fields.StringField;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author xiongyx
 * @date 2021/2/1
 *
 * todo 单测用例不要使用临时文件解耦
 */
public class DBFileIOTest {

    @Test
    public void testSerialize() throws IOException {
        String tableId = "people";
        TableDesc tableDesc = new TableDesc(
                tableId,
                new ColumnTypeEnum[]{ ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.STRING_TYPE}
        );

        HeapPageId pageId = new HeapPageId(tableId,1);

        Record record1 = new Record();
        record1.setRecordId(new RecordId(pageId,1));
        record1.setTableDesc(tableDesc);
        record1.setFieldList(Arrays.asList(
                new IntField(1),
                new IntField(11),
                new StringField("a1"))
        );

        Record record2 = new Record();
        record2.setRecordId(new RecordId(pageId,2));
        record2.setTableDesc(tableDesc);
        record2.setFieldList(Arrays.asList(
                new IntField(2),
                new IntField(22),
                new StringField("a2"))
        );

        Record record3 = new Record();
        record3.setRecordId(new RecordId(pageId,3));
        record3.setTableDesc(tableDesc);
        record3.setFieldList(Arrays.asList(
                new IntField(3),
                new IntField(33),
                new StringField("a3"))
        );

        // 测试插入、删除
        DBPage dbHeapPage = new DBHeapPage(tableDesc,pageId,new byte[Database.getBufferPool().getPageSize()]);
        dbHeapPage.insertRecord(record1);
        dbHeapPage.insertRecord(record2);
        dbHeapPage.insertRecord(record3);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),3);

        dbHeapPage.deleteRecord(record3);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),2);

        // 测试序列化/反序列化
        byte[] bytes = dbHeapPage.serialize();
        DBPage dbHeapPageCopy = new DBHeapPage(tableDesc,pageId,bytes);
        Assert.assertEquals(dbHeapPageCopy.getNotEmptySlotsNum(),2);
        dbHeapPageCopy.insertRecord(record3);
        Assert.assertEquals(dbHeapPageCopy.getNotEmptySlotsNum(),3);
    }

    @Test
    public void testSerializeFullPage() throws IOException {
        String tableId = "people";
        TableDesc tableDesc = new TableDesc(
                "people",
                new ColumnTypeEnum[]{ ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.STRING_TYPE}
        );

        HeapPageId pageId = new HeapPageId(tableId,1);
        // 测试插入、删除
        DBPage dbHeapPage = new DBHeapPage(tableDesc,pageId,new byte[Database.getBufferPool().getPageSize()]);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),0);

        int maxSlot = dbHeapPage.getMaxSlotNum();
        for(int i=0; i<maxSlot; i++){
            Record record = new Record();
            record.setRecordId(new RecordId(pageId,i));
            record.setTableDesc(tableDesc);
            record.setFieldList(Arrays.asList(
                    new IntField(i),
                    new IntField(10 + i),
                    new StringField("a" + i))
            );
            dbHeapPage.insertRecord(record);
        }

        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),maxSlot);

        // 测试序列化/反序列化
        byte[] bytes = dbHeapPage.serialize();
        DBPage dbHeapPageCopy = new DBHeapPage(tableDesc,pageId,bytes);
        Assert.assertEquals(dbHeapPageCopy.getNotEmptySlotsNum(),maxSlot);

        Record recordNeedDelete = new Record();
        recordNeedDelete.setRecordId(new RecordId(pageId,3));
        recordNeedDelete.setTableDesc(tableDesc);
        dbHeapPageCopy.deleteRecord(recordNeedDelete);
        Assert.assertEquals(dbHeapPageCopy.getNotEmptySlotsNum(),maxSlot-1);
    }

    @Test
    public void testFileWrite(){
        String tableId = "people";
        TableDesc tableDesc = new TableDesc(
                "people",
                new ColumnTypeEnum[]{ ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.STRING_TYPE}
        );

        HeapPageId pageId = new HeapPageId(tableId,1);
        // 测试插入、删除
        DBPage dbHeapPage = new DBHeapPage(tableDesc,pageId,new byte[Database.getBufferPool().getPageSize()]);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),0);

        int maxSlot = dbHeapPage.getMaxSlotNum();
        for(int i=0; i<maxSlot; i++){
            Record record = new Record();
            record.setRecordId(new RecordId(pageId,i));
            record.setTableDesc(tableDesc);
            record.setFieldList(Arrays.asList(
                    new IntField(i),
                    new IntField(10 + i),
                    new StringField("a" + i))
            );
            dbHeapPage.insertRecord(record);
        }

        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),maxSlot);

        File file = new File("table1");
        DBFile table1File = new DBHeapFile(tableDesc,file);

        table1File.writePage(dbHeapPage);
        DBPage dbHeapPageCopy = table1File.readPage(dbHeapPage.getPageId());

        Assert.assertEquals(dbHeapPageCopy.getNotEmptySlotsNum(),maxSlot);
    }
}
