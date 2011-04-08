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

package voldemort;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.io.FileUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import voldemort.client.RoutingTier;
import voldemort.client.protocol.RequestFormatFactory;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.AbstractSocketService;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.server.http.StoreServlet;
import voldemort.server.niosocket.NioSocketService;
import voldemort.server.protocol.RequestHandler;
import voldemort.server.protocol.RequestHandlerFactory;
import voldemort.server.protocol.SocketRequestHandlerFactory;
import voldemort.server.socket.SocketService;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.UnreachableStoreException;
import voldemort.store.http.HttpStore;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.ImmutableList;

/**
 * Helper functions for testing with real server implementations
 * 
 * 
 */
public class ServerTestUtils {

    public static StoreRepository getStores(String storeName, String clusterXml, String storesXml) {
        StoreRepository repository = new StoreRepository();
        Store<ByteArray, byte[]> store = new InMemoryStorageEngine<ByteArray, byte[]>(storeName);
        repository.addLocalStore(store);
        repository.addRoutedStore(store);

        // create new metadata store.
        MetadataStore metadata = createMetadataStore(new ClusterMapper().readCluster(new StringReader(clusterXml)),
                                                     new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml)));
        repository.addLocalStore(metadata);
        return repository;
    }

    public static VoldemortConfig getVoldemortConfig() {
        File temp = TestUtils.createTempDir();
        VoldemortConfig config = new VoldemortConfig(0, temp.getAbsolutePath());
        new File(config.getMetadataDirectory()).mkdir();
        return config;
    }

    public static AbstractSocketService getSocketService(boolean useNio,
                                                         String clusterXml,
                                                         String storesXml,
                                                         String storeName,
                                                         int port) {
        RequestHandlerFactory factory = getSocketRequestHandlerFactory(clusterXml,
                                                                       storesXml,
                                                                       getStores(storeName,
                                                                                 clusterXml,
                                                                                 storesXml));
        return getSocketService(useNio, factory, port, 5, 10, 10000);
    }

    public static RequestHandlerFactory getSocketRequestHandlerFactory(String clusterXml,
                                                                       String storesXml,
                                                                       StoreRepository storeRepository) {

        return new SocketRequestHandlerFactory(null,
                                               storeRepository,
                                               createMetadataStore(new ClusterMapper().readCluster(new StringReader(clusterXml)),
                                                                   new StoreDefinitionsMapper().readStoreList(new StringReader(storesXml))),
                                               null,
                                               null,
                                               null);
    }

    public static AbstractSocketService getSocketService(boolean useNio,
                                                         RequestHandlerFactory requestHandlerFactory,
                                                         int port,
                                                         int coreConnections,
                                                         int maxConnections,
                                                         int bufferSize) {
        AbstractSocketService socketService = null;

        if(useNio) {
            socketService = new NioSocketService(requestHandlerFactory,
                                                 port,
                                                 bufferSize,
                                                 coreConnections,
                                                 "client-request-service",
                                                 false);
        } else {
            socketService = new SocketService(requestHandlerFactory,
                                              port,
                                              coreConnections,
                                              maxConnections,
                                              bufferSize,
                                              "client-request-service",
                                              false);
        }

        return socketService;
    }

    public static SocketStore getSocketStore(String storeName, int port) {
        return getSocketStore(storeName, port, RequestFormatType.VOLDEMORT_V1);
    }

    public static SocketStore getSocketStore(String storeName, int port, RequestFormatType type) {
        return getSocketStore(storeName, "localhost", port, type);
    }

    public static SocketStore getSocketStore(String storeName,
                                             String host,
                                             int port,
                                             RequestFormatType type) {
        return getSocketStore(storeName, host, port, type, false);
    }

    public static SocketStore getSocketStore(String storeName,
                                             String host,
                                             int port,
                                             RequestFormatType type,
                                             boolean isRouted) {
        SocketPool socketPool = new SocketPool(2, 10000, 100000, 32 * 1024);
        return new SocketStore(storeName,
                               new SocketDestination(host, port, type),
                               socketPool,
                               isRouted);
    }

    public static Context getJettyServer(String clusterXml,
                                         String storesXml,
                                         String storeName,
                                         RequestFormatType requestFormat,
                                         int port) throws Exception {
        StoreRepository repository = getStores(storeName, clusterXml, storesXml);

        // initialize servlet
        Server server = new Server(port);
        server.setSendServerVersion(false);
        Context context = new Context(server, "/", Context.NO_SESSIONS);

        RequestHandler handler = getSocketRequestHandlerFactory(clusterXml, storesXml, repository).getRequestHandler(requestFormat);
        context.addServlet(new ServletHolder(new StoreServlet(handler)), "/stores");
        server.start();
        return context;
    }

    public static HttpStore getHttpStore(String storeName, RequestFormatType format, int port) {
        return new HttpStore(storeName,
                             "localhost",
                             port,
                             new HttpClient(),
                             new RequestFormatFactory().getRequestFormat(format),
                             false);
    }

    /**
     * Return a free port as chosen by new ServerSocket(0)
     */
    public static int findFreePort() {
        return findFreePorts(1)[0];
    }

    /**
     * Return an array of free ports as chosen by new ServerSocket(0)
     */
    public static int[] findFreePorts(int n) {
        int[] ports = new int[n];
        ServerSocket[] sockets = new ServerSocket[n];
        try {
            for(int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
            return ports;
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            for(int i = 0; i < n; i++) {
                try {
                    if(sockets[i] != null)
                        sockets[i].close();
                } catch(IOException e) {}
            }
        }
    }

    public static Cluster getLocalCluster(int numberOfNodes) {
        return getLocalCluster(numberOfNodes, findFreePorts(3 * numberOfNodes), null);
    }

    public static Cluster getLocalCluster(int numberOfNodes, int[][] partitionMap) {
        return getLocalCluster(numberOfNodes, findFreePorts(3 * numberOfNodes), partitionMap);
    }

    public static Cluster getLocalCluster(int numberOfNodes, int[] ports, int[][] partitionMap) {
        if(3 * numberOfNodes != ports.length)
            throw new IllegalArgumentException(3 * numberOfNodes + " ports required but only "
                                               + ports.length + " given.");
        List<Node> nodes = new ArrayList<Node>();
        for(int i = 0; i < numberOfNodes; i++) {
            List<Integer> partitions = ImmutableList.of(i);
            if(null != partitionMap) {
                partitions = new ArrayList<Integer>(partitionMap[i].length);
                for(int p: partitionMap[i]) {
                    partitions.add(p);
                }
            }

            nodes.add(new Node(i,
                               "localhost",
                               ports[3 * i],
                               ports[3 * i + 1],
                               ports[3 * i + 2],
                               partitions));
        }

        return new Cluster("test-cluster", nodes);
    }

    public static Node getLocalNode(int nodeId, List<Integer> partitions) {
        int[] ports = findFreePorts(3);
        return new Node(nodeId, "localhost", ports[0], ports[1], ports[2], partitions);
    }

    public static MetadataStore createMetadataStore(Cluster cluster, List<StoreDefinition> storeDefs) {
        Store<String, String> innerStore = new InMemoryStorageEngine<String, String>("inner-store");
        innerStore.put(MetadataStore.CLUSTER_KEY,
                       new Versioned<String>(new ClusterMapper().writeCluster(cluster)));
        innerStore.put(MetadataStore.STORES_KEY,
                       new Versioned<String>(new StoreDefinitionsMapper().writeStoreList(storeDefs)));

        return new MetadataStore(innerStore, 0);
    }

    public static List<StoreDefinition> getStoreDefs(int numStores) {
        List<StoreDefinition> defs = new ArrayList<StoreDefinition>();
        SerializerDefinition serDef = new SerializerDefinition("string");
        for(int i = 0; i < numStores; i++)
            defs.add(new StoreDefinitionBuilder().setName("test" + i)
                                                 .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                 .setKeySerializer(serDef)
                                                 .setValueSerializer(serDef)
                                                 .setRoutingPolicy(RoutingTier.SERVER)
                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                 .setReplicationFactor(2)
                                                 .setPreferredReads(1)
                                                 .setRequiredReads(1)
                                                 .setPreferredWrites(1)
                                                 .setRequiredWrites(1)
                                                 .build());
        return defs;
    }

    public static StoreDefinition getStoreDef(String storeName,
                                              int replicationFactor,
                                              int preads,
                                              int rreads,
                                              int pwrites,
                                              int rwrites,
                                              String strategyType) {
        SerializerDefinition serDef = new SerializerDefinition("string");
        return new StoreDefinitionBuilder().setName(storeName)
                                           .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                           .setKeySerializer(serDef)
                                           .setValueSerializer(serDef)
                                           .setRoutingPolicy(RoutingTier.SERVER)
                                           .setRoutingStrategyType(strategyType)
                                           .setReplicationFactor(replicationFactor)
                                           .setPreferredReads(preads)
                                           .setRequiredReads(rreads)
                                           .setPreferredWrites(pwrites)
                                           .setRequiredWrites(rwrites)
                                           .build();
    }

    public static HashMap<ByteArray, byte[]> createRandomKeyValuePairs(int numKeys) {
        HashMap<ByteArray, byte[]> map = new HashMap<ByteArray, byte[]>();
        for(int cnt = 0; cnt <= numKeys; cnt++) {
            int keyInt = (int) (Math.random() * 100000);
            ByteArray key = new ByteArray(ByteUtils.getBytes("" + keyInt, "UTF-8"));
            byte[] value = ByteUtils.getBytes("value-" + keyInt, "UTF-8");

            map.put(key, value);
        }

        return map;
    }

    public static HashMap<String, String> createRandomKeyValueString(int numKeys) {
        HashMap<String, String> map = new HashMap<String, String>();
        for(int cnt = 0; cnt <= numKeys; cnt++) {
            int keyInt = (int) (Math.random() * 100000);
            map.put("" + keyInt, "value-" + keyInt);
        }

        return map;
    }

    public static VoldemortConfig createServerConfig(boolean useNio,
                                                     int nodeId,
                                                     String baseDir,
                                                     String clusterFile,
                                                     String storeFile,
                                                     Properties properties) throws IOException {
        Props props = new Props(properties);
        props.put("node.id", nodeId);
        props.put("voldemort.home", baseDir + "/node-" + nodeId);
        props.put("bdb.cache.size", 1 * 1024 * 1024);
        props.put("bdb.write.transactions", "true");
        props.put("bdb.flush.transactions", "true");
        props.put("jmx.enable", "false");
        props.put("enable.mysql.engine", "true");

        VoldemortConfig config = new VoldemortConfig(props);
        config.setMysqlDatabaseName("voldemort");
        config.setMysqlUsername("voldemort");
        config.setMysqlPassword("voldemort");
        config.setStreamMaxReadBytesPerSec(10 * 1000 * 1000);
        config.setStreamMaxWriteBytesPerSec(10 * 1000 * 1000);

        config.setUseNioConnector(useNio);

        // clean and reinit metadata dir.
        File tempDir = new File(config.getMetadataDirectory());
        tempDir.mkdirs();
        tempDir.deleteOnExit();

        File tempDir2 = new File(config.getDataDirectory());
        tempDir2.mkdirs();
        tempDir2.deleteOnExit();

        // copy cluster.xml / stores.xml to temp metadata dir.
        if(null != clusterFile)
            FileUtils.copyFile(new File(clusterFile),
                               new File(tempDir.getAbsolutePath() + File.separatorChar
                                        + "cluster.xml"));
        if(null != storeFile)
            FileUtils.copyFile(new File(storeFile), new File(tempDir.getAbsolutePath()
                                                             + File.separatorChar + "stores.xml"));

        return config;
    }

    public static AdminClient getAdminClient(Cluster cluster) {

        AdminClientConfig config = new AdminClientConfig();
        return new AdminClient(cluster, config);
    }

    public static AdminClient getAdminClient(String bootstrapURL) {
        AdminClientConfig config = new AdminClientConfig();
        return new AdminClient(bootstrapURL, config);
    }

    public static RequestHandlerFactory getSocketRequestHandlerFactory(StoreRepository repository) {
        return new SocketRequestHandlerFactory(null, repository, null, null, null, null);
    }

    public static void stopVoldemortServer(VoldemortServer server) throws IOException {
        try {
            server.stop();
        } finally {
            FileUtils.deleteDirectory(new File(server.getVoldemortConfig().getVoldemortHome()));
        }
    }

    public static VoldemortServer startVoldemortServer(VoldemortConfig config, Cluster cluster) {
        VoldemortServer server = new VoldemortServer(config, cluster);
        server.start();

        ServerTestUtils.waitForServerStart(server.getIdentityNode());
        // wait till server start or throw exception
        return server;
    }

    public static void waitForServerStart(Node node) {
        boolean success = false;
        int retries = 10;
        Store<ByteArray, ?> store = null;
        while(retries-- > 0) {
            store = ServerTestUtils.getSocketStore(MetadataStore.METADATA_STORE_NAME,
                                                   node.getSocketPort());
            try {
                store.get(new ByteArray(MetadataStore.CLUSTER_KEY.getBytes()));
                success = true;
            } catch(UnreachableStoreException e) {
                store.close();
                store = null;
                System.out.println("UnreachableSocketStore sleeping will try again " + retries
                                   + " times.");
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e1) {
                    // ignore
                }
            }
        }

        store.close();
        if(!success)
            throw new RuntimeException("Failed to connect with server:" + node);
    }
}
