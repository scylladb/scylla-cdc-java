import org.apache.commons.lang3.RandomStringUtils;
import shaded.com.scylladb.cdc.driver3.driver.core.Cluster;
import shaded.com.scylladb.cdc.driver3.driver.core.Session;

import java.net.InetSocketAddress;

public class InsertData {

    private static Integer counter=0;
    private static Session getSession(){

        InetSocketAddress finalAddress = new InetSocketAddress("localhost",9041);
        Cluster clusterName = Cluster.builder()
                .withClusterName("dview-cluster")
                .withCredentials("admin","admin")
                .addContactPointsWithPorts(finalAddress)
                .build();
        Session session = clusterName.connect();

        return session;
    }

    public static String generateRandomStrings(){
        int length = 10;
        boolean useLetters = true;
        boolean useNumbers = false;
        String generatedString = RandomStringUtils.random(length, useLetters, useNumbers);

        return generatedString;
    }

    public static void insertQueryEmployee(Session session,int id) {
        int min = 1000;
        int max = 1000000;
        int salary = (int)(Math.random()*(max-min+1)+min);
        String employeeCode = generateRandomStrings();
        String employeeQuery = "INSERT INTO testing.employee (id, salary, code) VALUES (" + id + "," + salary + ",'" + employeeCode + "')";
        System.out.println(employeeQuery);
        session.execute(employeeQuery);
    }

    public static void insertQueryStudent(Session session,int rollNumber){
        int min = 1;
        int max = 100;
        int marks = (int)(Math.random()*(max-min+1)+min);
        String studentCode = generateRandomStrings();
        String studentQuery = "INSERT INTO testing.student (roll_no, marks, code) VALUES (" + rollNumber + "," + marks + ",'" + studentCode + "')";
        System.out.println(studentQuery);
        session.execute(studentQuery);
    }


    public static void main(String[] args) throws InterruptedException {
       Session session = getSession();
       for(int i=1;i<=1000;i++){
           insertQueryEmployee(session, i);
           if(i%10 == 0)
               insertQueryStudent(session, i);
           else
               insertQueryStudent(session, i);
       }
    }
}
