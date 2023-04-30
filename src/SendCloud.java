import com.mongodb.*;
import org.eclipse.paho.client.mqttv3.*;

import java.util.*;
import java.io.*;
import javax.swing.*;

public class SendCloud  implements MqttCallback  {
    static MqttClient mqttclient;
    static MongoClient mongoClient;
    static DB db;
    static DBCollection mongocol;
    static String mongo_user = new String();
    static String mongo_password = new String();
    static String mongo_address = new String();
    static String cloud_server = new String();
    static String cloud_topic = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    static String mongo_database = new String();
    static String mongo_collection = new String();
    static String mongo_authentication = new String();
    //    static String cloud_server = new String();
//    static String cloud_topic = new String();
    static JTextArea textArea = new JTextArea(10, 50);

    public static void publishSensor(String leitura) {
        try {
            MqttMessage mqtt_message = new MqttMessage();
            mqtt_message.setPayload(leitura.getBytes());
            mqttclient.publish(cloud_topic, mqtt_message);
        } catch (MqttException e) {
            e.printStackTrace();}
    }

//    private static void createWindow() {
//        JFrame frame = new JFrame("Send to Cloud");
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        JLabel textLabel = new JLabel("Data to send do broker: ",SwingConstants.CENTER);
//        JButton b1 = new JButton("Send Data");
//        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
//        frame.getContentPane().add(textArea, BorderLayout.CENTER);
//        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
//        frame.setLocationRelativeTo(null);
//        frame.pack();
//        frame.setVisible(true);
//        b1.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent evt) {
//                //System.exit(0);
//                publishSensor(textArea.getText());
//            }
//        });
//    }

//    private void createWindow() {
//        // Connect to MongoDB
////        MongoClient mongoClient = new MongoClient("localhost", 27017);
////        MongoDatabase database = mongoClient.getDatabase("myDatabase");
////        MongoCollection<Document> collection = database.getCollection("myCollection");
//        DBCollection collection = connectMongo();
//
//        // Connect to MQTT broker
//        try {
//            MqttClient mqttClient = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
//            mqttClient.connect();
//
//            // Get data from MongoDB and send it to MQTT broker
//            FindIterable<Document> iterable = collection.find();
//            for (Document doc : iterable) {
//                String data = doc.toJson();
//                MqttMessage message = new MqttMessage(data.getBytes());
//                mqttClient.publish("myTopic", message);
//            }
//
//            // Disconnect from databases
//            mqttClient.disconnect();
//            mongoClient.close();
//
//        } catch (MqttSecurityException e) {
//            e.printStackTrace();
//        } catch (MqttException e) {
//            e.printStackTrace();
//        }
//    }

    private static void createWindow() {
        // Connect to MongoDB and get the collection
        DBCollection collection = connectMongo();

        // Connect to MQTT broker
        try {
            MqttClient mqttClient = new MqttClient("tcp://localhost:1883", MqttClient.generateClientId());
            mqttClient.connect();

            // Get data from MongoDB and send it to MQTT broker
            DBCursor cursor = collection.find();
            while (cursor.hasNext()) {
                String data = cursor.next().toString();
                MqttMessage message = new MqttMessage(data.getBytes());
                mqttClient.publish("myTopic", message);
            }

            // Disconnect from databases
            mqttClient.disconnect();

        } catch (MqttSecurityException e) {
            e.printStackTrace();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {

        try {
            Properties p = new Properties();
            p.load(new FileInputStream("SendCloud.ini"));
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            cloud_server = p.getProperty("cloud_server");
            cloud_topic = p.getProperty("cloud_topic");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_collection = p.getProperty("mongo_collection");
//            cloud_server = p.getProperty("cloud_server");
//            cloud_topic = p.getProperty("cloud_topic");
        } catch (Exception e) {
            System.out.println("Error reading SendCloud.ini file " + e);
            JOptionPane.showMessageDialog(null, "The SendCloud.ini file wasn't found.", "Send Cloud", JOptionPane.ERROR_MESSAGE);
        }
//        new SendCloud().connecCloud();
        new CloudToMongo().connectMongo();
        createWindow();

    }

//    public void connecCloud() {
//        try {
//            mqttclient = new MqttClient(cloud_server, "SimulateSensor"+cloud_topic);
//            mqttclient.connect();
//            mqttclient.setCallback(this);
//            mqttclient.subscribe(cloud_topic);
//        } catch (MqttException e) {
//            e.printStackTrace();
//        }
//    }

    public static DBCollection connectMongo() {
        String mongoURI = new String();
        mongoURI = "mongodb://";
        if (mongo_authentication.equals("true")) mongoURI = mongoURI + mongo_user + ":" + mongo_password + "@";
        mongoURI = mongoURI + mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true")) mongoURI = mongoURI + "/?replicaSet=" + mongo_replica+"&authSource=admin";
            else mongoURI = mongoURI + "/?replicaSet=" + mongo_replica;
        else
        if (mongo_authentication.equals("true")) mongoURI = mongoURI  + "/?authSource=admin";
        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        db = mongoClient.getDB(mongo_database);
        mongocol = db.getCollection(mongo_collection);
        return mongocol;
    }


    @Override
    public void connectionLost(Throwable cause) {    }
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) { }
    @Override
    public void messageArrived(String topic, MqttMessage message){ }

}
