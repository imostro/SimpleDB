package simpledb.expression;

import simpledb.structure.Tuple;
import simpledb.parser.Predicate;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The field index into the first tuple in the predicate
     */
    private int fIdx1;

    /**
     * The field index into the second tuple in the predicate
     */
    private int fIdx2;

    /**
     * The operation to apply (as defined in Predicate.Op);
     * either Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     * Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ,
     * or Predicate.Op.LESS_THAN_OR_EQ
     * field2 – The field index into the second tuple in the predicate
     */
    private Predicate.Op op;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.fIdx1 = field1;
        this.fIdx2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(fIdx1).compare(op, t2.getField(fIdx2));
    }

    public int getIndex1()
    {
        // some code goes here
        return fIdx1;
    }

    public int getIndex2()
    {
        // some code goes here
        return fIdx2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
