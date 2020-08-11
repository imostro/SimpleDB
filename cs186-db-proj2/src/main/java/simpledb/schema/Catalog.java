package simpledb.schema;

import simpledb.structure.DbFile;
import simpledb.structure.HeapFile;
import simpledb.structure.TupleDesc;
import simpledb.structure.Type;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

    /**
     * 用于存储表名映射文件
     * key 表名
     */
    private final HashMap<String, DbStruct> dbFileMap = new HashMap<String, DbStruct>();

    /**
     * 用于存储表Id映射文件
     * key 表Id
     * value 表结构体
     */
    private final HashMap<Integer, DbStruct> dbFileIdMap = new HashMap<Integer, DbStruct>();

    private static class DbStruct {
        Integer dbId;   /*表ID*/
        String tName;   /*表名*/
        DbFile dbFile;  /*表文件*/
        String pKeyField;   /*表的属性前缀键*/

        public DbStruct(Integer dbId, String tName, DbFile dbFile) {
            this(dbId, tName, dbFile, "");
        }

        public DbStruct(Integer dbId, String tName, DbFile dbFile, String pKeyField) {
            this.dbId = dbId;
            this.tName = tName;
            this.dbFile = dbFile;
            this.pKeyField = pKeyField;
        }
    }
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        DbStruct struct = new DbStruct(file.getId(), name, file, pkeyField);
        dbFileMap.put(name, struct);
        dbFileIdMap.put(file.getId(), struct);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here

        DbStruct struct = dbFileMap.getOrDefault(name, null);
        if (struct == null){
            throw new NoSuchElementException();
        }
        return struct.dbId;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        DbStruct struct = dbFileIdMap.getOrDefault(tableid, null);
        if (struct == null){
            throw new NoSuchElementException();
        }
        return struct.dbFile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if(!dbFileIdMap.containsKey(tableid)){
            throw new NoSuchElementException();
        }
        return dbFileIdMap.get(tableid).dbFile;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if(!dbFileIdMap.containsKey(tableid)){
            return "";
        }
        return dbFileIdMap.get(tableid).pKeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return dbFileIdMap.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if(!dbFileIdMap.containsKey(id)){
            return "";
        }
        return dbFileIdMap.get(id).tName;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        dbFileIdMap.clear();
        dbFileMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";

        String baseFolder=new File(catalogFile).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
