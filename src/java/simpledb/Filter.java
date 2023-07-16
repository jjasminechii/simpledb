package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator childOp;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.childOp = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.childOp.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.childOp.open();
    }

    public void close() {
        super.close();  // not sure if needed
        this.childOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // this.childOp.rewind();
        open();
        close();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (this.childOp.hasNext()) {
            Tuple tuple = this.childOp.next();
            if (this.predicate.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.childOp};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.childOp = children[0];
    }

}
