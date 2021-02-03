import org.junit.Assert;
import org.junit.Test;
import simpledb.dbfile.DBHeapFile;
import simpledb.dbpage.DBHeapPage;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.dbrecord.RecordId;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.fields.StringField;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class DemoTest {

    @Test
    public void testWriteDisk() throws IOException {
        String tableId = "people";
        TableDesc tableDesc = new TableDesc(
                "people",
                Arrays.asList(
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.STRING_TYPE
                )
        );

        PageId pageId = new PageId(tableId,1);

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
        DBHeapPage dbHeapPage = new DBHeapPage(tableDesc,pageId,new byte[4096]);
        dbHeapPage.insertRecord(record1);
        dbHeapPage.insertRecord(record2);
        dbHeapPage.insertRecord(record3);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),3);

        dbHeapPage.deleteRecord(record3);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),2);

        // 测试序列化/反序列化 有bug todo
        byte[] bytes = dbHeapPage.serialize();
        DBHeapPage dbHeapPageCopy = new DBHeapPage(tableDesc,pageId,bytes);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),2);
        dbHeapPageCopy.insertRecord(record3);
        Assert.assertEquals(dbHeapPage.getNotEmptySlotsNum(),3);

        // todo 测试满页的序列化/反序列化
    }
}
