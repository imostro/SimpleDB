package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * TupleDesc describes the schema of a tuple.
 *
 * 描述元组信息类
 */
public class TupleDesc implements Serializable {

    /*
        元组中每个字段的类型和名字
     */
    private TDItem[] tdItems;

    /*
        字段个数
     */
    private int numFields;

    /*
        元组大小
     */
    private int size;

    /**
     * A help class to facilitate organizing the information of each field
     *
     * 帮助元组类来组织每个字段的详细信息
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        numFields = typeAr.length;
        tdItems = new TDItem[numFields];
        for (int i = 0; i < numFields; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        numFields = typeAr.length;
        tdItems = new TDItem[numFields];
        for (int i = 0; i < numFields; i++) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    public TupleDesc(TDItem[] tdItems) {
        numFields = tdItems.length;
        this.tdItems = tdItems;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name == null){
            throw new NoSuchElementException();
        }
        String fieldName;
        for (int i = 0; i < numFields; i++) {
            if((fieldName = getFieldName(i)) !=null && fieldName.equals(name))   return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        size = 0;
        for (int i = 0; i < numFields; i++) {
            size += getFieldType(i).getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int len = td1.numFields() + td2.numFields();
        TDItem[] tdItems = Arrays.copyOf(td1.tdItems, len);
        for (int i = 0; i < td2.numFields(); i++) {
            tdItems[td1.numFields()+i] = td2.tdItems[i];
        }
        return new TupleDesc(tdItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc)) return false;
        TupleDesc desc = (TupleDesc) o;
        if(desc.numFields != this.numFields) return false;

        for (int i = 0; i < numFields; i++) {

            if(this.getFieldType(i) != desc.getFieldType(i))    return false;
            String fName1 = this.getFieldName(i), fName2 = desc.getFieldName(i);
            if(!Objects.equals(fName1, fName2))  return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
