/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.readonly.mr;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.cluster.Node;
import voldemort.routing.ConsistentRoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteUtils;

/**
 * A base class that can be used for building voldemort read-only stores. To use
 * it you need to override the makeKey and makeValue methods which specify how
 * to construct the key and value from the values given in map().
 * 
 * The values given by makeKey and makeValue will then be serialized with the
 * appropriate voldemort Serializer.
 * 
 * 
 */
public abstract class AbstractHadoopStoreBuilderMapper<K, V> extends
        AbstractStoreBuilderConfigurable implements Mapper<K, V, BytesWritable, BytesWritable> {

    private MessageDigest md5er;
    private ConsistentRoutingStrategy routingStrategy;
    private Serializer<Object> keySerializer;
    private Serializer<Object> valueSerializer;
    private CompressionStrategy valueCompressor;
    private CompressionStrategy keyCompressor;
    private SerializerDefinition keySerializerDefinition;
    private SerializerDefinition valueSerializerDefinition;

    public abstract Object makeKey(K key, V value);

    public abstract Object makeValue(K key, V value);

    /**
     * Create the voldemort key and value from the input key and value and map
     * it out for each of the responsible voldemort nodes
     * 
     * The output key is the md5 of the serialized key returned by makeKey().
     * The output value is the nodeid of the responsible node followed by
     * serialized value returned by makeValue().
     */
    public void map(K key,
                    V value,
                    OutputCollector<BytesWritable, BytesWritable> output,
                    Reporter reporter) throws IOException {
        byte[] keyBytes = keySerializer.toBytes(makeKey(key, value));
        byte[] valBytes = valueSerializer.toBytes(makeValue(key, value));

        // compress key and values if required
        if(keySerializerDefinition.hasCompression()) {
            keyBytes = keyCompressor.deflate(keyBytes);
        }

        if(valueSerializerDefinition.hasCompression()) {
            valBytes = valueCompressor.deflate(valBytes);
        }

        // copy the bytes into an array with 4 additional bytes for the node id
        byte[] nodeIdAndValue = new byte[valBytes.length + 4];
        System.arraycopy(valBytes, 0, nodeIdAndValue, 4, valBytes.length);

        BytesWritable outputKey = new BytesWritable(md5er.digest(keyBytes));
        List<Node> nodes = routingStrategy.routeRequest(keyBytes);
        for(Node node: nodes) {
            ByteUtils.writeInt(nodeIdAndValue, node.getId(), 0);
            BytesWritable outputVal = new BytesWritable(nodeIdAndValue);

            output.collect(outputKey, outputVal);
        }
        md5er.reset();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(JobConf conf) {
        super.configure(conf);

        md5er = ByteUtils.getDigest("md5");
        keySerializerDefinition = getStoreDef().getKeySerializer();
        valueSerializerDefinition = getStoreDef().getValueSerializer();

        try {
            SerializerFactory factory = new DefaultSerializerFactory();

            if(conf.get("serializer.factory") != null) {
                factory = (SerializerFactory) Class.forName(conf.get("serializer.factory"))
                                                   .newInstance();
            }

            keySerializer = (Serializer<Object>) factory.getSerializer(keySerializerDefinition);
            valueSerializer = (Serializer<Object>) factory.getSerializer(valueSerializerDefinition);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        keyCompressor = new CompressionStrategyFactory().get(keySerializerDefinition.getCompression());
        valueCompressor = new CompressionStrategyFactory().get(valueSerializerDefinition.getCompression());

        routingStrategy = new ConsistentRoutingStrategy(getCluster().getNodes(),
                                                        getStoreDef().getReplicationFactor());
    }
}
