package simpledb;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * The aggregation operator to use
     */
    private Aggregator.Op aop;
    /**
     * The DbIterator that is feeding us tuples.
     */
    private DbIterator child;
    /**
     * The column over which we are computing an aggregate.
     */
    private int aggField;

    /**
     * The column over which we are grouping the result, or -1 if there is no grouping
     */
    private int groupField;

    /**
     * the tuple desc of aggregation
     */
    private TupleDesc td;

    /**
     * the tuple desc of iterator
     */
    private TupleDesc child_td;

    /**
     * 聚合的结果通过此AggregatorIterator访问
     */
    private DbIterator aggregateIter;

    /**
     * 真正的聚合操作时发生在聚合器Aggregator中的，而聚合的结果在iterator()方法的返回值中
     */
    private Aggregator aggregator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggField = afield;
        this.groupField = gfield;
        this.aop = aop;
        this.child_td = child.getTupleDesc();

        Type aField = child.getTupleDesc().getFieldType(afield);

        if(gfield != -1){
            // 需要优化
            Type gField = child.getTupleDesc().getFieldType(gfield);
            this.td = new TupleDesc(new Type[]{gField,aField}, new String[]{child_td.getFieldName(gfield), child_td.getFieldName(afield)});
            if(aField == Type.INT_TYPE){
                aggregator = new IntegerAggregator(groupField, gField, afield, aop);
            }else{
                aggregator = new StringAggregator(groupField, gField, afield, aop);
            }
        }else{
            this.td = new TupleDesc(new Type[]{aField});
            if(aField == Type.INT_TYPE){
                aggregator = new IntegerAggregator(groupField, null, afield, aop);
            }else{
                aggregator = new StringAggregator(groupField, null, afield, aop);
            }
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if(this.groupField<0){
            return null;
        }
        return child_td.getFieldName(groupField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child_td.getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        while (child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggregateIter = aggregator.iterator();
        aggregateIter.open();
    }



    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(aggregateIter.hasNext()){
            return aggregateIter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        aggregateIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
        aggregateIter.close();
        aggregateIter = null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (children.length > 0){
            child = children[0];
        }
    }

}
