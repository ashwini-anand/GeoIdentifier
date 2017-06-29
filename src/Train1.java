import opennlp.tools.namefind.*;
import opennlp.tools.util.InputStreamFactory;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.TrainingParameters;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Scanner;

/**
 * Created by asanand on 6/13/17.
 */
public class Train1 {

    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    static final int NUMBEROFROWS=100;
    private static String outputFile = "../resources/en-customlocation.bin";

    public static void main(String[] args) throws Exception {
        if(args.length < 2){
            throw new Exception("Incorrect number of arguments. Please give arguments database name followed by table name");
        }
        Scanner in = new Scanner(System.in);
        System.out.println("Enter number of columns having geo location information");
        int n = in.nextInt();
        System.out.println("Enter column numbers (0 indexed)");
        HashSet<Integer> colIdx = new HashSet<>();
        for(int i=0; i<n; i++){
            colIdx.add(in.nextInt());
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
        ResultSetMetaData rsmd = res.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        String trainStr = ""; //in real scenario avoid this String (for scalability reason). Save required things one by one to a file and then feed that file as inputstream to train method
        while(res.next()){
            for(int i=1; i<=columnsNumber;i++){
                if(colIdx.contains(i-1)){
                    trainStr += "<START:location> "+res.getString(i)+" <END> ";
                }else{
                    trainStr += res.getString(i)+" ";
                }
            }
        }
        InputStream stream = new ByteArrayInputStream(trainStr.getBytes(StandardCharsets.UTF_8));
        InputStreamFactory isf  = new InputStreamFactory() {
            @Override
            public InputStream createInputStream() throws IOException {
                return stream;
            }
        };
        ObjectStream<String> lineStream = new PlainTextByLineStream(isf,StandardCharsets.UTF_8);
        ObjectStream<NameSample> sampleStream = new NameSampleDataStream(lineStream);
        TokenNameFinderModel model;
        TokenNameFinderFactory tokenNameFinderFactory = new TokenNameFinderFactory();

        try{
            model = NameFinderME.train("en","location",sampleStream, TrainingParameters.defaultParams(),tokenNameFinderFactory);
        }finally {
            sampleStream.close();
        }
        BufferedOutputStream modelOut =null;

        try{
            modelOut = new BufferedOutputStream(new FileOutputStream(outputFile));
            model.serialize(modelOut);
        }finally {
            if(modelOut != null){
                modelOut.close();
            }
        }

    }
}
