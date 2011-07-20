package voldemort.store.analyzer;

import voldemort.server.AbstractService;
import voldemort.server.ServiceType;

public class AnalyzerService extends AbstractService {

    private final AnalyzerServer server;

    public AnalyzerService(int port, int maxConnections, int socketBufferSize) {
        super(ServiceType.ANALYZER);
        this.server = new AnalyzerServer(port, maxConnections, socketBufferSize);
    }

    public void init() {

    }

    @Override
    protected void startInner() {
        server.start();
    }

    @Override
    protected void stopInner() {
        this.server.shutdown();
    }
}
