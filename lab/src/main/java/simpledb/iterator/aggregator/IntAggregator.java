package simpledb.iterator.aggregator;

import simpledb.dbrecord.Record;
import simpledb.exception.DBException;
import simpledb.iterator.enums.AggregatorOpEnum;
import simpledb.iterator.operator.DbIterator;
import simpledb.matadata.fields.Field;
import simpledb.matadata.fields.FieldFactory;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;

import java.util.*;

/**
 * @author xiongyx
 * @date 2021/2/7
 */
public class IntAggregator implements Aggregator{

    private final int aggregatorFieldIndex;
    private final int groupByFieldIndex;
    private final ColumnTypeEnum groupByFieldColumnType;
    private final AggregatorOpEnum aggregatorOpEnum;
    private final TreeMap<Object, List<Integer>>  aggregateGroups;
    private final List<Integer> noGroupList;

    public IntAggregator(int aggregatorFieldIndex, int groupByFieldIndex,
                         ColumnTypeEnum groupByFieldColumnType, AggregatorOpEnum aggregatorOpEnum) {
        this.aggregatorFieldIndex = aggregatorFieldIndex;
        this.groupByFieldIndex = groupByFieldIndex;
        this.groupByFieldColumnType = groupByFieldColumnType;
        this.aggregatorOpEnum = aggregatorOpEnum;
        this.aggregateGroups = new TreeMap<>();
        this.noGroupList = new ArrayList<>();
    }

    private boolean isNoGroup(){
        return this.groupByFieldIndex == Aggregator.NO_GROUPING;
    }

    @Override
    public void mergeNewRecord(Record record) {
        Integer needAggregatorValue = ((IntField)record.getField(aggregatorFieldIndex)).getValue();
        if(isNoGroup()){
            // 不需要group by的聚合
            noGroupList.add(needAggregatorValue);
        }else{
            Object groupKey = record.getField(groupByFieldIndex).getValue();
            assert groupKey.getClass().equals(groupByFieldColumnType.javaType());

            List<Integer> groupValueList = aggregateGroups.get(groupKey);
            if(groupValueList == null){
                List<Integer> newGroupValueList = new ArrayList<>();
                newGroupValueList.add(needAggregatorValue);
                aggregateGroups.put(groupKey, newGroupValueList);
            }else {
                groupValueList.add(needAggregatorValue);
            }
        }
    }

    @Override
    public DbIterator iterator() {
        return new IntAggregatorItr();
    }

    //================================Integer字段group聚合操作迭代器===================================

    private class IntAggregatorItr implements DbIterator {

        private final List<Record> aggregatorResultList;
        private Iterator<Record> aggregatorResultItr;

        private int calculateResultByAggregatorOp(List<Integer> list){
            if (list.isEmpty()){
                System.out.println("IntAggregatorItr: warn list is empty");
                return 0;
            }

            AggregatorOpEnum aggregatorOpEnum = IntAggregator.this.aggregatorOpEnum;
            switch (aggregatorOpEnum){
                case MAX: {
                    int maxResult = list.get(0);
                    for (Integer item : list) {
                        if (item > maxResult) {
                            maxResult = item;
                        }
                    }
                    return maxResult;
                }
                case MIN: {
                    int minResult = list.get(0);
                    for (Integer item : list) {
                        if (item < minResult) {
                            minResult = item;
                        }
                    }
                    return minResult;
                }
                case AVG:{
                    int sum = getTotal(list);
                    return sum/list.size();
                }
                case SUM:{
                    return getTotal(list);
                }
                case COUNT:{
                    return list.size();
                }
                default:
                    throw new DBException("un support aggregatorOpEnum:" + aggregatorOpEnum);
            }
        }

        private int getTotal(List<Integer> list){
            return list.stream().mapToInt(item->item).sum();
        }

        public IntAggregatorItr() {
            if(isNoGroup()){
                Record record = new Record(getTupleDesc());
                int result = this.calculateResultByAggregatorOp(IntAggregator.this.noGroupList);
                record.setFieldList(Collections.singletonList(new IntField(result)));

                this.aggregatorResultList = Collections.singletonList(record);
            }else{
                List<Record> resultList = new ArrayList<>();
                // 分组收集
                for(Map.Entry<Object, List<Integer>> entry : IntAggregator.this.aggregateGroups.entrySet()){
                    Record record = new Record(getTupleDesc());
                    Field groupKeyField = FieldFactory.getFieldByClass(groupByFieldColumnType,entry.getKey());

                    int result = this.calculateResultByAggregatorOp(entry.getValue());
                    Field aggregateValField = new IntField(result);

                    record.setFieldList(Arrays.asList(
                            groupKeyField,aggregateValField)
                    );

                    resultList.add(record);
                }
                this.aggregatorResultList = resultList;
            }
        }

        @Override
        public TableDesc getTupleDesc() {
            if(isNoGroup()){
                return new TableDesc(Collections.singletonList(ColumnTypeEnum.INT_TYPE));
            }else {
                return new TableDesc(
                        Arrays.asList(IntAggregator.this.groupByFieldColumnType,ColumnTypeEnum.INT_TYPE)
                );
            }
        }

        @Override
        public void open() {
            this.aggregatorResultItr = this.aggregatorResultList.iterator();
        }

        @Override
        public void close() {
            this.aggregatorResultItr = null;
        }

        @Override
        public void reset() {
            if (this.aggregatorResultItr == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            this.aggregatorResultItr = this.aggregatorResultList.iterator();
        }

        @Override
        public boolean hasNext() {
            if (this.aggregatorResultItr == null) {
                throw new IllegalStateException("IntegerAggregator not open");

            }
            return this.aggregatorResultItr.hasNext();
        }

        @Override
        public Record next() {
            if (this.aggregatorResultItr == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            return this.aggregatorResultItr.next();
        }
    }
}
