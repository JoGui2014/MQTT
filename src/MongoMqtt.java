import com.mongodb.*;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;



import java.util.*;
import java.time.*;
import java.time.format.DateTimeFormatter;

import java.util.Vector;
import java.io.File;
import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.concurrent.TimeUnit;

public class MongoMqtt  implements MqttCallback  {
    static MqttClient mqttclient;
    static DBCursor cursor;
    static DBCursor cursoraux;
    static DB db;
    static DBCollection mongocol;
    static String cloud_server = new String();
    static String cloud_topic = new String();
    static String mongo_user = new String();
    static String mongo_password = new String();
    static String mongo_address = new String();
    static String mongo_host = new String();
    static String mongo_replica = new String();
    static String mongo_database = new String();
    static String mongo_collection = new String();
    static String mongo_authentication = new String();
    static JTextArea documentLabel = new JTextArea("\n");
    static JTextArea textArea = new JTextArea(10, 50);

    public static void publishSensor(String leitura, JButton b1) {
        try {
            MqttMessage mqtt_message = new MqttMessage();
            mqtt_message.setPayload(leitura.getBytes());
            mqtt_message.setRetained(true);
            mqtt_message.setQos(1);
            mqttclient.publish(cloud_topic, mqtt_message);
            sendMongoMQTT(b1, cursor);
        } catch (MqttException e) {
            e.printStackTrace();}
    }

    private static void createWindow() {
        JFrame frame = new JFrame("Send to Cloud");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Data to send do broker: ",SwingConstants.CENTER);
        JButton b1 = new JButton("Send Data");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(textArea, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        JScrollPane scroll = new JScrollPane (documentLabel, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        frame.setLocationRelativeTo(null);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.pack();
        frame.setVisible(true);
        cursoraux = mongocol.find();
        sendMongoMQTT(b1, cursor);
    }

    private static void sendMongoMQTT(JButton b1, DBCursor cursor) {

        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                //System.exit(0);
                publishSensor(textArea.getText(), b1);}
        });
        while(true){
            cursor = cursoraux;
        // Iterate over the document
            while (cursor.hasNext()) {
                DBObject document = cursor.next();
                textArea.append("Sensor: " + document.get("Sensor").toString() + " " + "Leitura: " + document.get("Leitura").toString() + "\n");
                System.out.println(textArea.getText());
                publishSensor(textArea.getText(), b1);
            }
            cursoraux = mongocol.find().skip(cursor.numSeen());
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {

        try {
            Properties p = new Properties();
            p.load(new FileInputStream("C:\\Users\\guiva\\OneDrive\\Documents\\ISCTE\\Terceiro ano ISCTE\\ES\\MQTT\\src\\SendCloud.ini"));
            cloud_server = p.getProperty("cloud_server");
            cloud_topic = p.getProperty("cloud_topic");
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            mongo_host = p.getProperty("mongo_host");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_collection = p.getProperty("mongo_collection");
        } catch (Exception e) {

            System.out.println("Error reading SendCloud.ini file " + e);
            JOptionPane.showMessageDialog(null, "The SendCloud.ini file wasn't found.", "Send Cloud", JOptionPane.ERROR_MESSAGE);
        }
        connectMongo();
        new MongoMqtt().connecCloud();
        createWindow();

    }

    public void connecCloud() {
        try {
            mqttclient = new MqttClient(cloud_server, "SimulateSensor"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static void connectMongo()  {

        String mongoURI = "mongodb://";

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
    }


    @Override
    public void connectionLost(Throwable cause) {    }
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) { }
    @Override
    public void messageArrived(String topic, MqttMessage message){ }

}
