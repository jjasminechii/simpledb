package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc td;
    private int track;
    private boolean check;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        this.check = false;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        this.check = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(!this.check) {
            this.check = true;
            int current = 0;
            while(this.child.hasNext()) {
                Tuple insert = this.child.next();
                try {
                    Database.getBufferPool().deleteTuple(this.t, insert);
                    current++;
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new DbException("BufferPool.deleteTuple failed");
                }
            }
            Tuple next = new Tuple(this.td);
            next.setField(0, new IntField(current));
            return next;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
