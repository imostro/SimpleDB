package simpledb.expression;

import simpledb.*;
import simpledb.statement.Operator;
import simpledb.structure.DbIterator;
import simpledb.expression.TransactionAbortedException;
import simpledb.structure.Tuple;
import simpledb.structure.TupleIterator;
import simpledb.parser.Predicate;
import simpledb.structure.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate predicate;

    private DbIterator child;

    private TupleDesc td;

    private TupleIterator filterTupleIterator;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        filterTupleIterator = filter();
        filterTupleIterator.open();
    }

    private TupleIterator filter(){
        ArrayList<Tuple> list = new ArrayList<>();
        try {
            while(child.hasNext()){
                Tuple tuple = child.next();
                if(predicate.filter(tuple)){
                    list.add(tuple);
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        return new TupleIterator(this.td, list);
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
        filterTupleIterator.close();
        filterTupleIterator = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        filterTupleIterator.rewind();
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
        // some code goes here
        if(filterTupleIterator.hasNext()) return filterTupleIterator.next();
            else   return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if(children[0] != child){
            child = children[0];
        }
    }

}
