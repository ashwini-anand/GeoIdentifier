/**
 * Created by asanand on 6/2/17.
 */
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;

public class GeoId1 {
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    static Column[] columns;
    static final int NUMBEROFROWS=100;
    static final int ACCURACY = 60;

    static class Column {
        ArrayList<String> contents = new ArrayList<>();
        int numOfGeoLocations =0;

        public String toString(){
            String str ="";
            for(int i=0; i<contents.size();i++){
                str += contents.get(i)+"\n";
            }
            str += "Total geoLocation present:"+numOfGeoLocations+"\n";
            str += "--------------This column is done-----------------";
            return str;
        }
    }

    public static void main(String[] args) throws Exception{

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
        columns = new Column[columnsNumber];
        for(int i=0; i<columns.length;i++){
            columns[i] = new Column();
        }
        while (res.next()){
            for(int i=1;i<=columnsNumber;i++){
                columns[i-1].contents.add(res.getString(i));
            }

        }
        getGeoInfo();
        ArrayList<Integer> result = new ArrayList<>();
        System.out.println("Number of Geo location in each column (out of "+NUMBEROFROWS+")");
        for(int i=0; i<columns.length;i++){
            System.out.println("column number="+i+"  Number of Geo location="+columns[i].numOfGeoLocations);
            if(columns[i].numOfGeoLocations > ACCURACY){
                result.add(i);
            }
        }
        System.out.println("\nFollowing columns contains geographical location info (0 based index) with accuracy"+ACCURACY);
        for(Integer i :result){
            System.out.print(i+" ");
        }
    }

    static void getGeoInfo() throws Exception{
        InputStream inputStreamTokenizer = new FileInputStream("../resources/en-token.bin");
        TokenizerModel tokenizerModel = new TokenizerModel(inputStreamTokenizer);
        TokenizerME tokenizerME = new TokenizerME(tokenizerModel);

        InputStream inputStreamNameFinder = new FileInputStream("../resources/en-ner-location.bin");
        TokenNameFinderModel tokenNameFinderModel = new TokenNameFinderModel(inputStreamNameFinder);
        NameFinderME nameFinderME = new NameFinderME(tokenNameFinderModel);

        for(int i=0; i<columns.length;i++){
            for(int j=0; j<NUMBEROFROWS;j++){
                String str = columns[i].contents.get(j);
                String[] tokens = tokenizerME.tokenize(str);
                Span[] nameSpans = nameFinderME.find(tokens);
                if(nameSpans.length >0){
                    columns[i].numOfGeoLocations++;
                }
            }
        }

    }
}
