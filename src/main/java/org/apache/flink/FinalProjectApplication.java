package org.apache.flink;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import sun.jvmstat.monitor.event.HostEvent;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FinalProjectApplication {

    static List<String[]> fpPatterns = new ArrayList<>();
    static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args)throws Exception{

        getFrequentPattern();

        DataStream<String> dataStream = env.socketTextStream("127.0.0.1", 7001)
                .flatMap(new LineTokenizer());

        anomalyDetect(dataStream);

        //execute program
        env.execute("WordCount Example");

//        for (String[] a:fpPatterns) {
////            for (String b:a){
////                System.out.print(b);
////            }
//            System.out.println(a[0]);
//        }


    }

    private static void getFrequentPattern() throws Exception{

        Socket s = new Socket("localhost",7000);
        BufferedReader bf = new BufferedReader(new InputStreamReader(s.getInputStream()));
        String [] wholePattern = bf.readLine().split(";"); //patternler ayrıldı

        for (String pattern:wholePattern) {

            pattern = pattern.substring(1,pattern.length()-1); //Başındaki ve sonundaki parantezler atıldı
            System.out.println(pattern);
            String[] patterns = pattern.split("',");
            for (int i=0;i<patterns.length;i++){
                patterns[i] = patterns[i].replaceAll(",","");
                patterns[i] = patterns[i].replaceAll("'","");
                patterns[i] = patterns[i].replaceAll("\\s","");
            }

//            for(String p : patterns){
//                System.out.println(p);
//            }
            fpPatterns.add(patterns); // listeye eklendi
        }

        s.close();
    }

    private static void anomalyDetect(DataStream<String> dataStream){


        Pattern<String, ?> patternForL =
                Pattern.<String>begin("begin").where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String s, Context<String> context) throws Exception {
                        boolean flag=false;
                        for (String[] a:fpPatterns) {
                            for (String b:a){
                                if(!s.equals(b)){
                                    flag=true;
                                }else {
                                    return false;
                                }
                            }
                        }

                        return flag;
                    }
                }).followedBy("middle").optional().where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String s, Context<String> context) throws Exception {
                        boolean flag=false;
                        for (String[] a:fpPatterns) {
                            for (String b:a){
                                if(!s.equals(b)){
                                    flag=true;
                                }else {
                                    return false;
                                }
                            }
                        }
                        for (String str:context.getEventsForPattern("begin")){
                            if(s.equals(str)){
                                return false;
                            }
                        }


                        return flag;
                    }
                }).followedBy("end").optional().where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        boolean flag=false;
                        for (String[] a:fpPatterns) {
                            for (String b:a){
                                if(!value.equals(b)){
                                    flag=true;
                                }else {
                                    return false;
                                }
                            }
                        }
                        for (String str:ctx.getEventsForPattern("begin")){
                            if(value.equals(str)){
                                return false;
                            }
                        }
                        for (String str:ctx.getEventsForPattern("middle")){
                            if(value.equals(str)){
                                return false;
                            }
                        }
                        return flag;
                    }
                }).within(Time.seconds(5));




        PatternStream<String> patternStream = CEP.pattern(dataStream, patternForL);

        DataStream<String> alerts = patternStream.select(new PatternSelectFunction<String, String>() {
            @Override
            public String select(Map<String, List<String>> matches) {

                String first = (matches.get("middle")==null) ? "" : matches.get("middle") + "";
                String second = (matches.get("end")==null) ? "" : matches.get("end") + "";
                return "Found: " +
                        matches.get("begin") +
                        first +
                        second;

            }

        });


        // emit result
        alerts.print();


    }
}
