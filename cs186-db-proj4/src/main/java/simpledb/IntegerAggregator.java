package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 分类域索引
     */
    private int gbfield;
    /**
     * 分类域类型
     */
    private Type gbfieldtype;

    /**
     * 聚合域索引
     */
    private int afield;
    /**
     * 聚合操作类型
     */
    private Op op;

    /**
     * 结果迭代器
     */
    private DbIterator resultIter;

    /**
     * 分组散列
     */
    private HashMap<Field, AggregatorItem> groupMap = new HashMap<>();

    /**
     * 分组表
     */
    private TupleDesc td;

    static class AggregatorItem {
        int sum;
        int count;
        int min;
        int max;
    }
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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if(gbfield == -1){
            AggregatorItem item = new AggregatorItem();
            item.max = Integer.MIN_VALUE; item.min = Integer.MAX_VALUE;
            groupMap.put(null, item);
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else{
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField aField = (IntField)tup.getField(afield);
        Field gField = null;
        if(gbfield != -1){
            gField = tup.getField(gbfield);
        }

        if(groupMap.containsKey(gField)){
            AggregatorItem item = groupMap.get(gField);
            item.count++;
            item.sum += aField.getValue();
            item.max = Math.max(aField.getValue(), item.max);
            item.min = Math.min(aField.getValue(), item.min);
        }else{
            AggregatorItem item = new AggregatorItem();
            item.sum = aField.getValue();
            item.count = 1;
            item.max = aField.getValue();
            item.min = aField.getValue();
            groupMap.put(gField, item);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        Iterator<Map.Entry<Field, AggregatorItem>> iterator =
                groupMap.entrySet().iterator();
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (iterator.hasNext()){
            Map.Entry<Field, AggregatorItem> next = iterator.next();
            Field key = next.getKey();
            AggregatorItem value = next.getValue();
            Tuple tuple = new Tuple(td);
            switch (op){
                case AVG:
                    tuple.setField(1, new IntField(value.sum/value.count));
                    break;
                case SUM:
                    tuple.setField(1, new IntField(value.sum));
                    break;
                case MIN:
                    tuple.setField(1, new IntField(value.min));
                    break;
                case MAX:
                    tuple.setField(1, new IntField(value.max));
                    break;
                case COUNT:
                    tuple.setField(1, new IntField(value.count));
                    break;
            }
            if(gbfield != -1){
                tuple.setField(0, key);
            }
            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }
}
