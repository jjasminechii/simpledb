package simpledb;
import java.util.Map;
import java.util.*;

public class AggregatorIterator implements OpIterator {
    private List<Tuple> tuples;
    private TupleDesc td;
    private Iterator<Tuple> tupleIterator;
    private Aggregator.Op what;

    public AggregatorIterator(Type gbField, boolean groupBy, Map<Field, List<Field>> groups,
                           Map<Field, Integer> currentGroup, Map<Field, Integer> count, Aggregator.Op what) {
        this.tuples = new ArrayList<>();
        if(groupBy) {
            this.td = new TupleDesc(new Type[]{gbField, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }
        for(Field groupField : groups.keySet()) {
            int aggVal = currentGroup.get(groupField);
            int countVal = count.get(groupField);
            if (what == Aggregator.Op.AVG) {
                aggVal = aggVal / countVal;
            }
            Tuple t = new Tuple(td);
            if(groupBy) {
                t.setField(0, groupField);
                t.setField(1, new IntField(aggVal));
            } else {
                t.setField(0, new IntField(aggVal));
            }
            this.tuples.add(t);
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.tupleIterator = this.tuples.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.tupleIterator != null && this.tupleIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (this.tupleIterator == null) {
            throw new NoSuchElementException();
        }
        return this.tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void close() {
        this.tupleIterator = null;
    }
}