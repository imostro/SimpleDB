package simpledb.expression;

import simpledb.expression.Aggregator;
import simpledb.structure.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

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

    private TupleDesc td;

    private HashMap<Field, Integer> groupMap = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw  new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        if(gbfield == -1){
            groupMap.put(null, 0);
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField = null;
        if (gbfield != -1){
            gField = tup.getField(gbfield);
        }
        if(groupMap.containsKey(gField)){
            groupMap.put(gField, groupMap.get(gField)+1);
        }else{
            groupMap.put(gField, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        Iterator<Map.Entry<Field, Integer>> iterator =
                groupMap.entrySet().iterator();
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (iterator.hasNext()){
            Map.Entry<Field, Integer> next = iterator.next();
            Field key = next.getKey();
            Integer value = next.getValue();
            Tuple tuple = new Tuple(td);
            tuple.setField(1, new IntField(value));
            if(gbfield != -1){
                tuple.setField(0, key);
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
