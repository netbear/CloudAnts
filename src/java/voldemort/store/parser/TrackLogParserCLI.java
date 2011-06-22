package voldemort.store.parser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import voldemort.client.ClientConfig;
import voldemort.store.analyzer.AnalyzerMaster;
import voldemort.store.analyzer.AnalyzerServer;

import com.google.common.collect.Maps;

public class TrackLogParserCLI {

    public static void main(String[] args) {

        if(args.length < 3) {
            System.out.println("Syntax: -logfile -client -date");
        }
        Map<Integer, String> files = Maps.newHashMap();
        files.put(0, args[0]);

        System.out.println("client: " + args[1]);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
        Date d = null;
        try {
            d = sdf.parse(args[2]);
        } catch(ParseException e) {
            e.printStackTrace();
        }
        String client = args[1];
        System.out.println("date: " + d);

        TrackLogParser parser = new TrackLogParser(files);
        parser.setDate(d);
        parser.setClient(client);

        AnalyzerServer server = new AnalyzerServer(8800, 4, 64000);
        server.start();
        AnalyzerMaster master = new AnalyzerMaster(new ClientConfig(), parser);
        master.addDestination("127.0.0.1", 8800);

        try {
            master.init();
        } catch(IOException ex) {
            System.out.println("TrackLogParserCLI main: " + ex.toString());
        }

        server.shutdown();
    }
}
