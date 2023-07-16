package simpledb;
import java.util.Map;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> currentGroup;
    private Map<Field, Integer> currentCount;
    private Map<Field, List<Field>> groups;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.currentGroup = new HashMap<>();
        this.currentCount = new HashMap<>();
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField;
        if(this.gbfield == Aggregator.NO_GROUPING){
            groupField = null;
        } else {
            groupField = tup.getField(this.gbfield);
        }
        if(!this.groups.containsKey(groupField)){
            this.groups.put(groupField, new ArrayList<Field>());
        }
        List<Field> groupTuples = this.groups.get(groupField);
        groupTuples.add(tup.getField(this.afield));
        int newValue = ((IntField) tup.getField(this.afield)).getValue();
        if(!this.currentGroup.containsKey(groupField)){
            this.currentGroup.put(groupField, newValue);
            this.currentCount.put(groupField, 1);
        } else {
            int currentCount = this.currentCount.get(groupField);
            int currentValue = this.currentGroup.get(groupField);
            switch(this.what) {
                case MIN:
                    this.currentGroup.put(groupField, Math.min(currentValue, newValue));
                    break;
                case MAX:
                    this.currentGroup.put(groupField, Math.max(currentValue, newValue));
                    break;
                case SUM:
                    this.currentGroup.put(groupField, currentValue + newValue);
                    break;
                case AVG:
                    this.currentGroup.put(groupField, currentValue + newValue);
                    this.currentCount.put(groupField, currentCount + 1);
                    break;
                case COUNT:
                    this.currentGroup.put(groupField, currentCount + 1);
                    this.currentCount.put(groupField, currentCount + 1);
                    break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        boolean groupBy = false;
        if(this.gbfield != Aggregator.NO_GROUPING) {
            groupBy = true;
        }
        return new AggregatorIterator(gbfieldtype, groupBy, groups, currentGroup,currentCount, what);
    }
}
