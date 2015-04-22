package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;

import java.util.Properties;


/**
 * Created by nachiket on 15/3/15.
 */
public class Workloadg extends Workload{


    public static final String TABLENAME_PROPERTY="table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT="usertable";


    /**
     * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
     */
    public static final String READ_ALL_FIELDS_PROPERTY="readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT="true";

    boolean readallfields;

    /**
     * The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY="writeallfields";

    /**
     * The default value for the writeallfields property.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT="false";

    boolean writeallfields;


    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY="readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT="0.95";

    /**
     * The name of the property for the proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY="updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT="0.05";

    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY="insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT="0.0";

    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY="scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT="0.0";


    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY="fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT="10";


    /**
     * The name of the property for the proportion of transactions that are read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY="readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT="0.0";



    /**
     * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";

    /**
     * The default distribution of requests across the keyspace
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";



    public static String table;
    int fieldcount,recordcount;
    public static int keygenerator=0;

    //Empty constructor
    public Workloadg() {
    }

    @Override
    public void init(Properties p) throws WorkloadException {
        //super.init(p);
        table = p.getProperty(TABLENAME_PROPERTY,TABLENAME_PROPERTY_DEFAULT);

        fieldcount=Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY,FIELD_COUNT_PROPERTY_DEFAULT));
        double readproportion=Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,READ_PROPORTION_PROPERTY_DEFAULT));
        double updateproportion=Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY,UPDATE_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion=Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY,INSERT_PROPORTION_PROPERTY_DEFAULT));
        double scanproportion=Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY,SCAN_PROPORTION_PROPERTY_DEFAULT));
        double readmodifywriteproportion=Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY,READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
        recordcount=Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
        String requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);

        readallfields=Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY,READ_ALL_FIELDS_PROPERTY_DEFAULT));
        writeallfields=Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY,WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

        //images_data_path="";

    }

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
     * synchronized, since each thread has its own threadstate instance.
     *
     * @param db
     * @param threadstate
     */
    @Override
    public boolean doInsert(DB db, Object threadstate) {
        String dbkey = Integer.toString(generateNextKey());
        //HashMap<String, ByteIterator> values = buildValues();

        /*if (db.insert(table,dbkey,values) == 0)
            return true;
        else
            return false; */
        return false;
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
     * synchronized, since each thread has its own threadstate instance.
     *
     * @param db
     * @param threadstate
     * @return false if the workload knows it is done for this thread. Client will terminate the thread. Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read traces from a file, return true when there are more to do, false when you are done.
     */
    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        return false;
    }

    /**
     * Generates new key. It is sequential no. generation with given record count
     */
    public synchronized int generateNextKey() {
        int key;
        key = keygenerator+1;
        keygenerator = key;
        return key;
    }

}
