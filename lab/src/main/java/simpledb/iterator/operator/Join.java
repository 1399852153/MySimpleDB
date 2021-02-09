package simpledb.iterator.operator;

import simpledb.dbrecord.Record;
import simpledb.iterator.JoinPredicate;
import simpledb.matadata.fields.Field;
import simpledb.matadata.table.TableDesc;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiongyx
 * @date 2021/2/9
 */
public class Join implements DbIterator{

    private final JoinPredicate joinPredicate;

    private DbIterator child1;
    private Record left;

    private DbIterator child2;
    private Record right;

    private Record next = null;
    private boolean open = false;

    public Join(JoinPredicate joinPredicate, DbIterator child1, DbIterator child2) {
        this.joinPredicate = joinPredicate;

        this.child1 = child1;
        this.left = null;

        this.child2 = child2;
        this.right = null;
    }


    public String getJoinField1Name() {
        return child1.getTupleDesc().getColumn(joinPredicate.getField1Index()).getFieldName();
    }

    public String getJoinField2Name() {
        return child2.getTupleDesc().getColumn(joinPredicate.getField2Index()).getFieldName();
    }

    public TableDesc getTupleDesc() {
        return TableDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    @Override
    public void open() {
        this.open = true;
        child1.open();
        child2.open();
        if (child1.hasNext()) {
            left = child1.next();
        }
        if (child2.hasNext()) {
            right = child2.next();
        }
    }

    @Override
    public void close() {
        child1.close();
        child2.close();
        left = null;
        right = null;
        next = null;
        this.open = false;
    }

    @Override
    public void reset() {
        this.close();
        this.open();
    }

    @Override
    public boolean hasNext() {
        if(!this.open){
            return false;
        }

        if(this.next == null){
            this.next = next();
        }

        return next != null;
    }

    @Override
    public Record next() {
        if(this.next != null){
            Record next = this.next;
            this.next = null;
            return next;
        }
        if(left == null && right == null){
            return null;
        }

        while (true) {
            Record next = null;

            if(joinPredicate.filter(left,right)){
                // left和right匹配join的条件，构造left+right join之后的record记录
                next = new Record(this.getTupleDesc());
                List<Field> nextRecordFieldList = new ArrayList<>();
                nextRecordFieldList.addAll(left.getFieldList());
                nextRecordFieldList.addAll(right.getFieldList());
                next.setFieldList(nextRecordFieldList);
            }

            // 尝试寻找下一个匹配项
            if (child2.hasNext()) {
                // 优先从child2开始寻找
                right = child2.next();
            } else {
                // child2遍历完了一遍
                if (child1.hasNext()) {
                    // 尝试着从child1中获得下一项进行匹配
                    left = child1.next();
                    // 此时child2进行重制，从头开始一轮新的匹配
                    child2.reset();
                    if (child2.hasNext()) {
                        right = child2.next();
                    }
                } else {
                    // child1和child2都迭代完了
                    left = null;
                    right = null;
                    return next;
                }
            }

            if(next != null){
                return next;
            }
        }
    }
}
