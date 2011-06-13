package voldemort.store.parser;

import java.util.Map;

import com.google.common.collect.Maps;

public class TrackLogParserCLI {

    public static void main(String[] args) {

        Map<Integer, String> files = Maps.newHashMap();
        files.put(0, args[0]);
        TrackLogParser parser = new TrackLogParser(files);
        parser.init();
    }
}
