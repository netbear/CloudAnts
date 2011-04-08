/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.performance.benchmark;

import java.io.PrintStream;
import java.util.HashMap;

class Results {

    public Results(int ops, int minL, int maxL, long totL, int medL, int q95, int q99) {
        operations = ops;
        minLatency = minL;
        maxLatency = maxL;
        totalLatency = totL;
        medianLatency = medL;
        q99Latency = q99;
        q95Latency = q95;
    }

    public int operations = -1;
    public int minLatency = -1;
    public int maxLatency = -1;
    public long totalLatency = -1;
    public int medianLatency = -1;
    public int q99Latency = -1;
    public int q95Latency = -1;

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("OPERATIONS = " + operations);
        buffer.append("MINLATENCY = " + minLatency);
        buffer.append("MAXLATENCY = " + maxLatency);
        buffer.append("MEDIANLATENCY = " + medianLatency);
        buffer.append("Q95LATENCY = " + q95Latency);
        buffer.append("Q99LATENCY = " + q99Latency);

        return buffer.toString();

    }
}

public class Measurement {

    private String _name;

    public String getName() {
        return _name;
    }

    private int _buckets;
    private int[] histogram;
    private int histogramOverflow;
    private int windowOperations;
    private long windowTotalLatency;
    private int operations = 0;
    private int minLatency = -1;
    private int maxLatency = -1;
    private long totalLatency = 0;
    private HashMap<Integer, int[]> returnCodes;
    private boolean summaryOnly = false;

    public Measurement(String name, boolean summaryOnly) {
        this._name = name;
        this._buckets = 1000;
        this.histogram = new int[_buckets];
        this.histogramOverflow = 0;
        this.operations = 0;
        this.totalLatency = 0;
        this.windowOperations = 0;
        this.windowTotalLatency = 0;
        this.minLatency = -1;
        this.maxLatency = -1;
        this.returnCodes = new HashMap<Integer, int[]>();
        this.summaryOnly = summaryOnly;
    }

    public synchronized void reportReturnCode(int code) {
        Integer Icode = code;
        if(!returnCodes.containsKey(Icode)) {
            int[] val = new int[1];
            val[0] = 0;
            returnCodes.put(Icode, val);
        }
        returnCodes.get(Icode)[0]++;
    }

    public synchronized void measure(int latency) {
        if(latency >= _buckets) {
            histogramOverflow++;
        } else {
            histogram[latency]++;
        }
        operations++;
        totalLatency += latency;
        windowOperations++;
        windowTotalLatency += latency;

        if((minLatency < 0) || (latency < minLatency)) {
            minLatency = latency;
        }

        if((maxLatency < 0) || (latency > maxLatency)) {
            maxLatency = latency;
        }
    }

    public Results generateResults() {
        int median = 0, q95 = 0, q99 = 0;
        int opcounter = 0;
        boolean done95th = false, done50th = false;
        for(int i = 0; i < _buckets; i++) {
            opcounter += histogram[i];
            double currentQuartile = ((double) opcounter) / ((double) operations);
            if(!done50th && currentQuartile >= 0.50) {
                median = i;
                done50th = true;
            }
            if(!done95th && currentQuartile >= 0.95) {
                q95 = i;
                done95th = true;
            }
            if(currentQuartile >= 0.99) {
                q99 = i;
                break;
            }
        }
        return new Results(operations, minLatency, maxLatency, totalLatency, median, q95, q99);
    }

    public void printReport(PrintStream out) {

        Results result = generateResults();
        out.println("[" + getName() + "]\tOperations: " + operations);
        out.println("[" + getName() + "]\tAverage(ms): "
                    + (((double) totalLatency) / ((double) operations)));
        out.println("[" + getName() + "]\tMin(ms): " + minLatency);
        out.println("[" + getName() + "]\tMax(ms): " + maxLatency);
        out.println("[" + getName() + "]\tMedian(ms): " + result.medianLatency);
        out.println("[" + getName() + "]\t95th(ms): " + result.q95Latency);
        out.println("[" + getName() + "]\t99th(ms): " + result.q99Latency);

        if(!this.summaryOnly) {
            for(Integer I: returnCodes.keySet()) {
                int[] val = returnCodes.get(I);
                out.println("[" + getName() + "]\tReturn: " + I + "\t" + val[0]);
            }

            for(int i = 0; i < _buckets; i++) {
                out.println("[" + getName() + "]: " + i + "\t" + histogram[i]);
            }
            out.println("[" + getName() + "]: >" + _buckets + "\t" + histogramOverflow);
        }
    }
}
