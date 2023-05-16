import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.io.*;
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

public class RecieveMQTT_Mov implements MqttCallback {
    MqttClient mqttclient;
    static String cloud_server = new String();
    static String cloud_topic = new String();
    static JTextArea documentLabel = new JTextArea("\n");
    static Connection connTo;
    static String sql_database_connection_to = new String();
    static String sql_database_password_to= new String();
    static String sql_database_user_to= new String();
    static String  sql_table_to= new String();

    private static void createWindow2() {
        JFrame frame = new JFrame("Data Bridge");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Data : ",SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane (documentLabel,JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b1 = new JButton("Stop the program");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                System.exit(0);
            }

        });
    }

    public static void main(String[] args) {

        try {
            Properties p = new Properties();

            p.load(new FileInputStream("src\\ReceiveCloud.ini"));

            cloud_server = p.getProperty("cloud_server");
            cloud_topic = p.getProperty("cloud_topic_mov");
        } catch (Exception e) {
            System.out.println("Error reading ReceiveCloud.ini file " + e);
            JOptionPane.showMessageDialog(null, "The ReceiveCloud.inifile wasn't found.", "Receive Cloud", JOptionPane.ERROR_MESSAGE);
        }
        try{
            Properties b = new Properties();
            b.load(new FileInputStream("src\\WriteMysql.ini"));
            sql_table_to= b.getProperty("sql_table_to");
            sql_database_connection_to = b.getProperty("sql_database_connection_to");
            sql_database_password_to = b.getProperty("sql_database_password_to");
            sql_database_user_to= b.getProperty("sql_database_user_to");
        } catch (Exception e) {
            System.out.println("Error reading WriteMysql.ini file " + e);
            JOptionPane.showMessageDialog(null, "The WriteMysql inifile wasn't found.", "Data Migration", JOptionPane.ERROR_MESSAGE);
        }
        new RecieveMQTT_Mov().connecCloud();
        new RecieveMQTT_Mov().connectDatabase_to();
        System.out.println(documentLabel.getText());
        createWindow2();
    }

    public void connecCloud() {
        int i;
        try {
            i = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "ReceiveCloud"+String.valueOf(i)+"_"+cloud_topic);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage c)
            throws Exception {
        try {
            String payload = new String(c.getPayload());
            documentLabel.append(payload+"\n");
            WriteToMySQL(payload);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void connectDatabase_to() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connTo =  DriverManager.getConnection(sql_database_connection_to,sql_database_user_to,sql_database_password_to);
            documentLabel.setText("SQl Connection:"+sql_database_connection_to+"\n");
            documentLabel.append("Connection To MariaDB Destination " + sql_database_connection_to + " Suceeded"+"\n");
        } catch (Exception e){System.out.println("Mysql Server Destination down, unable to make the connection. "+e);}
    }

    public void WriteToMySQL (String messageMqtt){
//        System.out.println( "\"" + messageMqtt.split(" ")[3] + " " + messageMqtt.split(" ")[4] + "\"");
        String SqlCommando = "Insert into " + sql_table_to + "(`Hora`, `SalaEntrada`, `SalaSaida`, `isValid`)" + " " + "VALUES" + " " + "(" + "\"" + messageMqtt.split(" ")[2] + " " + messageMqtt.split(" ")[3] + "\"" + "," + messageMqtt.split(" ")[7] + "," + messageMqtt.split(" ")[11] + "," + messageMqtt.split(" ")[13] + ")";
        try {
            documentLabel.append(SqlCommando.toString()+"\n");
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
            Statement s = connTo.createStatement();
            int result = new Integer(s.executeUpdate(SqlCommando));
            s.close();
        } catch (Exception e){System.out.println("Error Inserting in the database . " + e); System.out.println(SqlCommando);}
    }
}