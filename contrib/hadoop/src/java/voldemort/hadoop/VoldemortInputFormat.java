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

package voldemort.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

public class VoldemortInputFormat extends InputFormat<ByteArray, Versioned<byte[]>> {

    /**
     * Create a new connection to admin client and give it to RecordReader.
     * Called on the TaskTracker
     */
    @Override
    public RecordReader<ByteArray, Versioned<byte[]>> createRecordReader(InputSplit currentSplit,
                                                                         TaskAttemptContext taskContext)
            throws IOException, InterruptedException {
        return new VoldemortRecordReader();
    }

    /**
     * One mapper for every node. Every InputSplit then connects to the
     * particular node. Called on JobClient
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String bootstrapURL = VoldemortHadoopConfig.getVoldemortURL(conf);
        String storeName = VoldemortHadoopConfig.getVoldemortStoreName(conf);

        AdminClient adminClient = new AdminClient(bootstrapURL, new AdminClientConfig());

        /**
         * TODO: To put check to see if store exists
         */
        Cluster cluster = adminClient.getAdminClientCluster();
        Collection<Node> nodes = cluster.getNodes();
        Iterator<Node> nodeIter = nodes.iterator();

        List<InputSplit> splits = new ArrayList<InputSplit>();
        while(nodeIter.hasNext()) {
            Node currentNode = nodeIter.next();
            VoldemortInputSplit split = new VoldemortInputSplit(storeName, currentNode);
            splits.add(split);
        }

        adminClient.stop();
        return splits;
    }
}
