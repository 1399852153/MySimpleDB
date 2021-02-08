import org.junit.Assert;
import org.junit.Before;
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
import simpledb.iterator.Predicate;
import simpledb.iterator.aggregator.Aggregator;
import simpledb.iterator.aggregator.IntAggregator;
import simpledb.iterator.enums.AggregatorOpEnum;
import simpledb.iterator.operator.DbIterator;
import simpledb.iterator.operator.SeqScan;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.iterator.operator.Filter;
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

    private String tableId;
    private TableDesc tableDesc;
    private DBFile table1File;

    @Before
    public void setUp(){
        tableId = "people";
        tableDesc = new TableDesc(
                "people",
                Arrays.asList(
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.INT_TYPE,
                        ColumnTypeEnum.STRING_TYPE
                )
        );

        File file = new File("table1");
        table1File = new DBHeapFile(tableDesc,file);
        Database.getCatalog().addTable(tableId,tableDesc,table1File);
    }

    @Test
    public void testDBFileIterator(){
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
                        new StringField("dbIterator a" + i + "-b" + j))
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
             System.out.println("testDBFileIterator：" + record);
        }
        Assert.assertFalse(dbFileIterator.hasNext());
        dbFileIterator.reset();
        Assert.assertTrue(dbFileIterator.hasNext());
    }

    @Test
    public void testSeqScan(){
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
                        new StringField("seqScan a" + i + "-b" + j))
                );
                dbHeapPage.insertRecord(record);
            }

            table1File.writePage(dbHeapPage);
        }

        SeqScan seqScan = new SeqScan(table1File);
        Assert.assertFalse(seqScan.hasNext());

        seqScan.open();
        Assert.assertTrue(seqScan.hasNext());

        while(seqScan.hasNext()){
            Record record = seqScan.next();
            // System.out.println("testFilter：" + record);
        }
        Assert.assertFalse(seqScan.hasNext());
        seqScan.reset();
        Assert.assertTrue(seqScan.hasNext());
    }

    @Test
    public void testFilterBase(){
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
                        new StringField("filter a" + i + "-b" + j))
                );
                dbHeapPage.insertRecord(record);
            }

            table1File.writePage(dbHeapPage);
        }

        SeqScan seqScan = new SeqScan(table1File);
        Filter filter = new Filter(
                new Predicate(OperatorEnum.EQUALS,new IntField(3),0),seqScan);

        Assert.assertFalse(filter.hasNext());

        filter.open();
        Assert.assertTrue(filter.hasNext());

        while(filter.hasNext()){
            Record record = filter.next();
            System.out.println("testFilter：" + record);
        }
        Assert.assertFalse(filter.hasNext());
        filter.reset();
        Assert.assertTrue(filter.hasNext());
    }

    @Test
    public void testFilterAdvance(){
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
                        new StringField("filter a" + i + "-b" + j))
                );
                dbHeapPage.insertRecord(record);
            }

            table1File.writePage(dbHeapPage);
        }

        SeqScan seqScan = new SeqScan(table1File);
        Filter filter = new Filter(
                new Predicate(OperatorEnum.GREATER_THAN_OR_EQ,new IntField(3),0),seqScan);
        // 嵌套filter
        Filter nextFilter = new Filter(
                new Predicate(OperatorEnum.LESS_THAN,new IntField(7),0),filter
        );

        nextFilter.open();
        Assert.assertTrue(nextFilter.hasNext());

        int matchNum = 0;
        while(nextFilter.hasNext()){
            Record record = nextFilter.next();
            matchNum++;
            System.out.println("testFilter：" + record);
        }
        // (>=3 && <7)
        Assert.assertEquals(matchNum,8);
        Assert.assertFalse(nextFilter.hasNext());
        nextFilter.reset();
        Assert.assertTrue(nextFilter.hasNext());
    }

    @Test
    public void testAggregator(){
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
                        new StringField("seqScan a" + i + "-b" + j))
                );
                dbHeapPage.insertRecord(record);
            }

            table1File.writePage(dbHeapPage);
        }

        SeqScan seqScan = new SeqScan(table1File);
        seqScan.open();

        Aggregator aggregator = new IntAggregator(
                0,1,ColumnTypeEnum.INT_TYPE, AggregatorOpEnum.SUM
        );
        while(seqScan.hasNext()){
            Record record = seqScan.next();
            aggregator.mergeNewRecord(record);
        }

        DbIterator dbIterator = aggregator.iterator();
        dbIterator.open();

        while(dbIterator.hasNext()){
            Record record = dbIterator.next();
            System.out.println(record);
        }

    }
}
