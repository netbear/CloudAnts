package voldemort.store.parser;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import voldemort.client.ClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.analyzer.AnalyzerMaster;
import voldemort.xml.ClusterMapper;

import com.google.common.collect.Maps;

public class TrackLogParserCLI {

    public static void main(String[] args) {

        if(args.length < 5) {
            System.out.println("Syntax: -logfile -client -date -cluster.xml -localnode");
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

        int localid = Integer.parseInt(args[4]);

        TrackLogParser parser = new TrackLogParser(files);
        parser.setDate(d);
        parser.setClient(client);

        // AnalyzerServer server = new AnalyzerServer(8800, 4, 64000);
        // server.start();

        ClusterMapper clusterMapper = new ClusterMapper();
        Cluster cluster = null;
        try {
            cluster = clusterMapper.readCluster(new File(args[3]));
        } catch(IOException ex) {
            System.out.println("reading cluster.xml failed");
        }

        AnalyzerMaster master = new AnalyzerMaster(new ClientConfig(), parser);
        for(Node node: cluster.getNodes())
            if(node.getId() != localid) {
                master.addDestination(node.getHost(), 7000);
                System.out.println(node.getHost() + "  added");
            }

        try {
            master.init();
        } catch(IOException ex) {
            System.out.println("TrackLogParserCLI main: " + ex.toString());
        }
        master.outputDirtyKeys();

        // server.shutdown();
    }
}
