package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator childOp;
    private OpIterator iter;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator actual;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.childOp = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        Type currentType = child.getTupleDesc().getFieldType(afield);
        if (currentType.equals(Type.INT_TYPE)) {
            // int type
            if (this.gfield != -1) {
                // groupby
                actual = new IntegerAggregator(this.gfield, childOp.getTupleDesc().getFieldType(this.gfield), this.afield, this.aop);
            } else {
                // not groupby
                actual = new IntegerAggregator(this.gfield, null, this.afield, this.aop);
            }
        } else {
            // string type
            if (this.gfield != -1) {
                // groupby
                actual = new StringAggregator(this.gfield, childOp.getTupleDesc().getFieldType(this.gfield), this.afield, this.aop);
            } else {
                // not groupby
                actual = new StringAggregator(this.gfield, null, this.afield, this.aop);
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    if (this.gfield == -1) {
            return Aggregator.NO_GROUPING;
        }
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    if (this.gfield == -1) {
            return null;
        }
        return childOp.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	    return this.childOp.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        childOp.open();
        while(childOp.hasNext()) {
            Tuple current = childOp.next();
            actual.mergeTupleIntoGroup(current);
        }
        iter = actual.iterator();
        iter.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // call aggregator's iterator's next() for next tuple (if hasNext() = true)
	    if (iter != null && iter.hasNext()) {
            return iter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childOp.rewind();
        iter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        Type[] fieldTypes;
        String[] fieldNames;
        if (afield == Aggregator.NO_GROUPING) {
            fieldTypes = new Type[]{childOp.getTupleDesc().getFieldType(afield)};
            fieldNames = new String[]{aop + "(" + childOp.getTupleDesc().getFieldName(afield) + ")"};
        } else {
            fieldTypes = new Type[]{childOp.getTupleDesc().getFieldType(gfield), childOp.getTupleDesc().getFieldType(afield)};
            fieldNames = new String[]{childOp.getTupleDesc().getFieldName(gfield), aop + "(" + childOp.getTupleDesc().getFieldName(afield) + ")"};
        }
        return new TupleDesc(fieldTypes, fieldNames);
//        return iter.getTupleDesc();
    }

    public void close() {
        childOp.close();
        iter.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    return new OpIterator[]{childOp};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.childOp = children[0];
    }
}
