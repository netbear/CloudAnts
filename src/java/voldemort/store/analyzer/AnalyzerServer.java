package voldemort.store.analyzer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.store.parser.AccessNode;
import voldemort.store.parser.ClientNode;
import voldemort.store.parser.TrackLogParser;
import voldemort.utils.ByteUtils;

import com.google.common.collect.Maps;

public class AnalyzerServer extends Thread {

    private final Logger logger;

    private static final Object SUCCESS = new Object();
    private final int port;
    private final int socketBufferSize;
    private final int maxThreads;

    private TrackLogParser parser;

    private ServerSocket serverSocket = null;

    private Map<String, ClientNode> clients = null;

    public AnalyzerServer(int port, int maxThreads, int socketBufferSize) {
        this.port = port;
        this.socketBufferSize = socketBufferSize;
        this.maxThreads = maxThreads;
        this.logger = Logger.getLogger(AnalyzerServer.class.getName());
        Map<Integer, String> files = Maps.newHashMap();
        files.put(0, "track.log");
        this.parser = new TrackLogParser(files);
    }

    @Override
    public void run() {
        logger.info("Starting voldemort analyzer server on port " + port);
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(port));
            serverSocket.setReceiveBufferSize(this.socketBufferSize);

            while(!isInterrupted() && !serverSocket.isClosed()) {
                final Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                socket.setSendBufferSize(this.socketBufferSize);

                DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                                          64000));
                DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                                              64000));

                negotiateProtocol(inputStream, outputStream);
                handleRequest(inputStream, outputStream);

            }
        } catch(BindException e) {
            logger.error("Could not bind to port " + port + ".");
            throw new VoldemortException(e);
        } catch(SocketException e) {
            // If we have been manually shutdown, ignore
            if(!isInterrupted())
                logger.error("Error in server: ", e);
        } catch(IOException e) {
            logger.info("AnalyzerServer :" + e.toString());
            throw new VoldemortException(e);
        } catch(Throwable t) {
            logger.error(t);
            if(t instanceof Error)
                throw (Error) t;
            else if(t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new VoldemortException(t);
        } finally {
            // logger.info("Closing server");
            if(serverSocket != null) {
                try {
                    serverSocket.close();
                } catch(IOException e) {
                    logger.warn("Error while shutting down server.", e);
                }
            }

        }
    }

    public void shutdown() {
        logger.info("Shutting down analyzer server...");
        interrupt();

        try {
            if(!serverSocket.isClosed()) {
                // logger.info("Closing socket in shutdown method");
                serverSocket.close();
            }
        } catch(IOException e) {
            logger.error("Error while closing socket server: " + e.getMessage());
        }
    }

    public void handleRequest(DataInputStream in, DataOutputStream out) throws IOException {
        int length = in.readByte();
        byte[] cmdBytes = new byte[length];
        boolean isUpdated = false;
        ByteUtils.read(in, cmdBytes);
        try {
            String cmd = ByteUtils.getString(cmdBytes, "UTF-8");
            logger.info("Reading : " + cmd);
            if(cmd.equals("init")) {
                parser.init();
                out.writeByte(4);
                out.write(ByteUtils.getBytes("done", "UTF-8"));
                out.flush();
            } else if(cmd.equals("update")) {
                List<ClientNode> masterClients = getClientFromBytes(in);
                Map<String, ClientNode> clients = parser.getClientNode();
                for(ClientNode client: masterClients) {
                    logger.info("receive clients: " + client);
                    if(clients.containsKey(client.getClient())) {
                        if(clients.get(client.getClient()).setVersion(client.getVersion())) {
                            isUpdated = true;
                        }
                    }
                }

                if(isUpdated) {
                    out.writeByte(1);
                    byte[] clientBytes = getClientBytes();
                    out.writeInt(clientBytes.length);
                    out.write(clientBytes);
                    out.flush();
                } else {
                    logger.info("no updates needed");
                    out.writeByte(0);
                }

                out.writeByte(4);
                out.write(ByteUtils.getBytes("done", "UTF-8"));
                out.flush();
            }

        } catch(IllegalArgumentException e) {

        } finally {
            outputDirtyKeys();
        }
    }

    public byte[] getClientBytes() {
        Collection<ClientNode> clients = parser.getClientNode().values();
        int length = 0;
        for(ClientNode c: clients) {
            length += c.sizeInBytes();
        }
        byte[] serialized = new byte[length];
        // ByteUtils.writeShort(serialized, (short) clients.size(), 0);
        int pos = 0;
        for(ClientNode c: clients) {
            System.arraycopy(c.toBytes(), 0, serialized, pos, c.sizeInBytes());
            pos += c.sizeInBytes();
        }
        return serialized;
    }

    public List<ClientNode> getClientFromBytes(DataInputStream in) throws IOException {
        int valueSize = in.readInt();
        byte[] valueBytes = new byte[valueSize];
        ByteUtils.read(in, valueBytes);
        int offset = 0;
        // logger.info("reading " + valueSize + " bytes.");
        ArrayList<ClientNode> clientList = new ArrayList<ClientNode>();
        while(offset < valueSize) {
            ClientNode client = new ClientNode(valueBytes, offset);
            // logger.info(client + " " + client.sizeInBytes() + " bytes.");
            offset += client.sizeInBytes();
            clientList.add(client);
        }

        return clientList;
    }

    private RequestFormatType negotiateProtocol(InputStream input, OutputStream output)
            throws IOException {
        input.mark(3);
        byte[] protoBytes = new byte[3];
        ByteUtils.read(input, protoBytes);
        RequestFormatType requestFormat;
        try {
            String proto = ByteUtils.getString(protoBytes, "UTF-8");
            requestFormat = RequestFormatType.fromCode(proto);
            output.write(ByteUtils.getBytes("ok", "UTF-8"));
            output.flush();
        } catch(IllegalArgumentException e) {
            // okay we got some nonsense. For backwards compatibility,
            // assume this is an old client who does not know how to negotiate
            requestFormat = RequestFormatType.VOLDEMORT_V0;
            // reset input stream so we don't interfere with request format
            input.reset();
            logger.info("No protocol proposal given, assuming "
                        + RequestFormatType.VOLDEMORT_V0.getDisplayName());
        }
        return requestFormat;
    }

    public void outputDirtyKeys() {
        Map<String, ClientNode> clients = parser.getClientNode();

        logger.info("dirty keys");
        for(ClientNode client: clients.values()) {
            ArrayList<AccessNode> nodeList = client.getClientOp();
            for(AccessNode node: nodeList) {
                if(node.isDirty()) {
                    String key = ByteUtils.getString(node.getKey().get(), "UTF-8");
                    logger.info(node.getClient() + " : " + key);
                } else {
                    String key = ByteUtils.getString(node.getKey().get(), "UTF-8");
                    logger.info(node.getClient() + " not dirty : " + key);
                }
            }
        }
    }
}
