/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;



/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton Mongo instance. */
    private static Mongo mongo;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    private static String mediaclass;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */

    /**  provide support for sharding. */
    private static final int [] _shardPorts = { 27018, 27019 };
    private ArrayList<String> _shardServers;

    /*public void setupClusterSharding() throws Exception {
        // Connect to mongos
        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "admin"));
        // Add the shards
        for (final int shardPort : _shardPorts) {
            final CommandResult result
                    = mongo.getDB("admin").command(new BasicDBObject("addshard", ("localhost:" + shardPort)));
            System.out.println(result);
    }*/

    private static File[] listOfFiles = null;
    private static long proportionValue, proportionCount = 0;

    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String url = props.getProperty("mongodb.url", "mongodb://localhost:27017");
            database = props.getProperty("mongodb.database", "ycsb");
            String writeConcernType = props.getProperty("mongodb.writeConcern", "safe").toLowerCase();
            final String maxConnections = props.getProperty("mongodb.maxconnections", "10");

            mediaclass=props.getProperty("mediaclass","false");
            if ("true".equals(mediaclass)) {
                if (!(new File("workload-data/").exists())) {
                    System.err.println("ERROR: Missing Path: 'workload-data' directory in ycsb directory");
                    System.exit(1);
                }
                listOfFiles = new File("workload-data/").listFiles();
            }

            if ("none".equals(writeConcernType)) {
                writeConcern = WriteConcern.NONE;
            }
            else if ("safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.SAFE;
            }
            else if ("normal".equals(writeConcernType)) {
                writeConcern = WriteConcern.NORMAL;
            }
            else if ("fsync_safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.FSYNC_SAFE;
            }
            else if ("replicas_safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.REPLICAS_SAFE;
            }
            else {
                System.err
                        .println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ none | safe | normal | fsync_safe | replicas_safe ]");
                System.exit(1);
            }

            try {
                // strip out prefix since Java driver doesn't currently support
                // standard connection format URL yet
                // http://www.mongodb.org/display/DOCS/Connections
                if (url.startsWith("mongodb://")) {
                    url = url.substring(10);
                }

                // need to append db to url.
                url += "/" + database;
                System.out.println("new database url = " + url);
                MongoOptions options = new MongoOptions();
                options.connectionsPerHost = Integer.parseInt(maxConnections);
                mongo = new Mongo(new DBAddress(url), options);

                System.out.println("mongo connection created with " + url);
            }
            catch (Exception e1) {
                System.err
                        .println("Could not initialize MongoDB connection pool for Loader: "
                                + e1.toString());
                e1.printStackTrace();
                return;
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            }
            catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
                return;
            }
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */

    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     *
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * Modified for Media data
     *
     * Insert a image record in the database if mediaclass is true.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);

            if ("true".equals(mediaclass)) {
                GridFS fs = new GridFS(db,"imageData");
                GridFSInputFile in = null;

                Random rn = new Random();
                int index = rn.nextInt(listOfFiles.length);

                File image = listOfFiles[index];
                if (image.exists()) {
                    in = fs.createFile(image);
                    in.setId(key);
                    in.setFilename(image.getName());
                    in.save();
                }
                return 0;
            }
            else {
                DBObject r = new BasicDBObject().append("_id", key);
                System.out.println("This is other than g");
                for (String k : values.keySet()) {
                    r.put(k, values.get(k).toArray());
                }
                WriteResult res = collection.insert(r, writeConcern);

                return res.getError() == null ? 0 : 1;
            }

        }
        catch (IOException e) {
            e.printStackTrace();
            return 1;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * Modified for Media data
     *
     * Read a image record from the database if mediaclass is true.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            if ("true".equals(mediaclass)) {

                GridFS gFS = new GridFS(db,"imageData");

                Random rn = new Random();
                int index = rn.nextInt(listOfFiles.length);

                File image = listOfFiles[index];
                GridFSDBFile data = gFS.findOne(image.getName());

                return data != null ? 0 : 1;
            }
            else {
                DBCollection collection = db.getCollection(table);
                DBObject q = new BasicDBObject().append("_id", key);
                DBObject fieldsToReturn = new BasicDBObject();

                DBObject queryResult = null;
                if (fields != null) {
                    Iterator<String> iter = fields.iterator();
                    while (iter.hasNext()) {
                        fieldsToReturn.put(iter.next(), INCLUDE);
                    }
                    queryResult = collection.findOne(q, fieldsToReturn);
                } else {
                    queryResult = collection.findOne(q);
                }

                if (queryResult != null) {
                    result.putAll(queryResult.toMap());
                }
                return queryResult != null ? 0 : 1;
            }
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            return res.getN() == 1 ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            DBCursor cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }

    }

    /**
     * TODO - Finish
     * 
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }


    /**
     * This is for Media data
     *
     * Insert a image record in the database. .
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values image to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public void insertImage(String table, String key,
                      HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);

            File[] listOfFiles = new File("workload-data/").listFiles();
            ArrayList<String> fileNames = new ArrayList<String>();

            GridFS fs = new GridFS(db);

            for (File image : listOfFiles) {
               if(image.exists()) {

                   byte[] imageBytes = LoadImage(image.getAbsolutePath());

                   GridFSInputFile in = fs.createFile(imageBytes);
                   in.setId(image.getName());
                   in.save();
               }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }


    /**
     * This is for Media data
     *
     * Read a image record in the database. .
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values image to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public void readImage(String table, String key,
                            HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongo.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);

            File[] listOfFiles = new File("workload-data/").listFiles();
            ArrayList<String> fileNames = new ArrayList<String>();

            GridFS fs = new GridFS(db);

            for (File image : listOfFiles) {
                if(image.exists()) {

                    byte[] imageBytes = LoadImage(image.getAbsolutePath());

                    GridFSInputFile in = fs.createFile(imageBytes);
                    in.setId(image.getName());
                    in.save();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    public byte[] LoadImage(String filePath) throws Exception {
        File file = new File(filePath);
        int size = (int)file.length();
        byte[] buffer = new byte[size];
        FileInputStream in = new FileInputStream(file);
        in.read(buffer);
        in.close();
        return buffer;
    }

}
