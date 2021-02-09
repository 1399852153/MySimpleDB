package simpledb.iterator.operator;

import simpledb.dbrecord.Record;
import simpledb.iterator.JoinPredicate;

/**
 * @author xiongyx
 * @date 2021/2/9
 */
public class Join {

    private final JoinPredicate joinPredicate;
    private DbIterator child1;
    private DbIterator child2;
    private Record left;
    private Record right;

    public Join(JoinPredicate joinPredicate, DbIterator child1, DbIterator child2) {
        this.joinPredicate = joinPredicate;
        this.child1 = child1;
        this.child2 = child2;
        this.left = null;
        this.right = null;
    }


    public String getJoinField1Name() {
//        return child1.getTupleDesc().getColumn(joinPredicate.getField1Index());
        return null;
    }


    public String getJoinField2Name() {
//        return child2.getTupleDesc().getFieldName(p.getField2());
        return null;
    }
}
