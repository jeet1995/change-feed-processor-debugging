package org.example.oai.memoryleak;

public class TestItem {

    private final String id;

    private final String pk;

    public TestItem(String id, String pk) {
        this.id = id;
        this.pk = pk;
    }

    public String getId() {
        return id;
    }

    public String getPk() {
        return pk;
    }
}
