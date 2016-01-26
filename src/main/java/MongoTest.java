import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.util.JSON;
import org.bson.BsonArray;
import org.bson.Document;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Filter;

/**
 * Created by root on 15-10-16.
 */
public class MongoTest {
    public static void main(String[] args) throws UnknownHostException, MongoException {
        MongoClient mg = new MongoClient( "10.1.1.134" , 27017 );

        //查询所有的Database
        for (String name : mg.getDatabaseNames()) {
            System.out.println("dbName: " + name);
        }

        MongoDatabase db = mg.getDatabase("local");
        //查询所有的聚集集合
        for (String name : db.listCollectionNames()) {
            System.out.println("collectionName: " + name);
        }

        MongoCollection startup_log = db.getCollection("startup_log");
        System.out.println(startup_log.find().limit(10));
        Document one = (Document)startup_log.find(Filters.eq("_id","10-1-1-134-144508256473")).first();
        System.out.println(one.toJson());
//        for(Object o :startup_log.find(Filters.eq("_id","10-1-1-134-1445082564741"))){
//            System.out.println(((Document) o).toJson());
//        }
//        for(Object o : startup_log.find().limit(10)){
//            System.out.println(((Document) o).toJson());
//        }
        //查询所有的数据
//        for (final Object index : startup_log.listIndexes()) {
//            System.out.println(((Document) index).toJson());
//        }

//        MongoDatabase test = mg.getDatabase("mydb");

//        test.createCollection("user");
//        MongoCollection user = test.getCollection("test3");
//        BsonArray ba = new BsonArray();
////        ba.add("1");
//        Document d = new Document("name", "name2").append("pos", 111);
//        user.updateMany(Filters.eq("name","name2"), new Document("$set",d), new UpdateOptions().upsert(true));
//        user.insertOne(d);

//        user.createIndex(new Document("id","text"));
//        Iterator it = user.find().into(new ArrayList()).iterator();
//        System.out.println(user.count());
//        while (it.hasNext()){
//            System.out.println(it.next());
//        }
//        for (final Object index : user.listIndexes()) {
//            System.out.println(((Document) index).toJson());
//        }
//        System.out.println(cur.count());
//        System.out.println(cur.getCursorId());
//        System.out.println(JSON.serialize(cur));
    }
    public void test(){
        MongoClient mg = new MongoClient( "10.1.1.134" , 27017 );
    }
}
