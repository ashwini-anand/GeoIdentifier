import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;

/**
 * Created by asanand on 6/14/17.
 */
public class GeoId2 {
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    static final int NUMBEROFROWS=100;

    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            throw new Exception("Incorrect number of arguments. Please give arguments database name followed by table name");
        }
        String db = args[0];
        String table = args[1];

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/"+db, "admin", "admin");
        Statement stmt = con.createStatement();
        ResultSet res = stmt.executeQuery("SELECT * FROM "+table+" LIMIT "+NUMBEROFROWS);
        System.out.println("Result:");
        ResultSetMetaData rsmd = res.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        int tokensLen = columnsNumber * NUMBEROFROWS;
        String[] tokens = new String[tokensLen];
        int k=0;

        while (res.next()){
            for(int i=1;i<=columnsNumber;i++){
                tokens[k] = res.getString(i);
                if(tokens[k]==null){
                    tokens[k] = " ";
                }
                k++;
            }
        }

        Span[] results = getGeoInfo(tokens);
        for(Span s: results){
            System.out.println(s.toString()+" "+tokens[s.getStart()]);
        }

    }

    public static Span[] getGeoInfo(String[] tokens) throws  Exception{
        InputStream inputStreamNameFinder = new FileInputStream("../resources/en-customlocation.bin");
        TokenNameFinderModel tokenNameFinderModel = new TokenNameFinderModel(inputStreamNameFinder);
        NameFinderME nameFinderME = new NameFinderME(tokenNameFinderModel);
        Span[] results = nameFinderME.find(tokens);
        return results;
    }

}
