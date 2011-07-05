package voldemort.store.parser;

public enum AccessType {
    GET("Get"),
    SET("Set");

    private String access;

    AccessType(String param) {
        access = param;
    }

    @Override
    public String toString() {
        return access;
    }
}
