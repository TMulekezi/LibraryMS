/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <2491633>
 *
 */

/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <2491633>
 *
 */
package com.example.finalassignment;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.



public class BooksDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome   = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public BooksDatabaseService(Socket aSocket){

        //TO BE COMPLETED
        serviceSocket = aSocket;
        this.start();

    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library

        String tmp = "";

        StringBuffer msg = new StringBuffer();
        try {

            //TO BE COMPLETED
            InputStream input = this.serviceSocket.getInputStream();
            InputStreamReader reader = new InputStreamReader(input);
            char x;

            while (true) {
                x = (char) reader.read();

                if (x == '#') {
                    break;
                }
                msg.append(x);
            }

            tmp = msg.toString();
            String[] temp = tmp.split(";");
            this.requestStr[0] = temp[0];
            this.requestStr[1] = temp[1];

        }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;

        this.outcome = null;

        String sql = ""; //TO BE COMPLETED- Update this line as needed.


        try {

            //Connet to the database
            //TO BE COMPLETED
            Class.forName("org.postgresql.Driver");
            Connection con = DriverManager.getConnection(Credentials.URL, Credentials.USERNAME, Credentials.PASSWORD);
            //Make the query
            //TO BE COMPLETED

            sql = "SELECT title, publisher, genre, rrp, COUNT(title) AS copies FROM " +
                    "(Select title, publisher, genre, rrp, bookid FROM " +
                    "author INNER JOIN book ON author.authorid = book.authorid " +
                    "WHERE familyname = ?) AS info1 " +
                    "INNER JOIN " +
                    "(SELECT bookid, copyid, onloan FROM library INNER JOIN bookcopy ON " +
                    "library.libraryid = bookcopy.libraryid WHERE city = ?) AS info2 " +
                    "ON info1.bookid = info2.bookid " +
                    "GROUP BY (title, publisher, genre, rrp)";
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setString(1, this.requestStr[0]);
            pstmt.setString(2, this.requestStr[1]);
            ResultSet rs = pstmt.executeQuery();

            //Process query
            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.

            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();

            crs.populate(rs);
           String serverOutput = "";
            while (crs.next()) {

                if (crs.isLast()) {
                    serverOutput+= crs.getObject("title") + " | " + crs.getObject("publisher") + " | " + crs.getObject("genre") +
                            " | " + crs.getObject("rrp") + " | " + crs.getObject("copies");
                } else {
                    serverOutput+= crs.getObject("title") + " | " + crs.getObject("publisher") + " | " + crs.getObject("genre") +
                            " | " + crs.getObject("rrp") + " | " + crs.getObject("copies") + "\n";
                }
            }
            System.out.println(serverOutput);
            crs.beforeFirst();

            //Clean up
            //TO BE COMPLETED
            this.outcome = crs;
            rs.close();
            pstmt.close();

        } catch (Exception e) { System.out.println(e);
            flagRequestAttended = false;
            System.exit(1);
        }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
            //Return outcome
            //TO BE COMPLETED
            OutputStream output = this.serviceSocket.getOutputStream();
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(output);
            outcomeStreamWriter.writeObject(this.outcome);
            outcomeStreamWriter.flush();


            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

            //Terminating connection of the service socket
            //TO BE COMPLETED
            this.serviceSocket.close();

        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
    }


    //The service thread run() method
    public void run()
    {
        try {
            System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
            System.exit(1);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
