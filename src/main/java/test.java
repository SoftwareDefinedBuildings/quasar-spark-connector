import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by almightykim on 5/9/15.
 */
public class test {


    // or use a connection string
    MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019");
    MongoClient mongoClient = new MongoClient(connectionString);

    MongoDatabase database = mongoClient.getDatabase("mydb");
    MongoCollection<Document> collection = database.getCollection("test");


    Document myDoc = collection.find(eq("i", 71)).first();




}
