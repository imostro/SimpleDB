package simpledb.systemtest;

import java.io.IOException;
import static org.junit.Assert.*;
import simpledb.*;
import simpledb.expression.Filter;
import simpledb.expression.TransactionAbortedException;
import simpledb.expression.TransactionId;
import simpledb.parser.Predicate;
import simpledb.structure.HeapFile;
import simpledb.structure.SeqScan;

public class FilterTest extends FilterBase {
    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException, IOException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        filter.open();

        int resultCount = 0;
        while (filter.hasNext()) {
            assertNotNull(filter.next());
            resultCount += 1;
        }

        filter.close();
        return resultCount;
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(FilterTest.class);
    }
}
