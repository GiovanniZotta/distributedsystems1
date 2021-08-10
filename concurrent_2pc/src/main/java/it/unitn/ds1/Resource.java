package it.unitn.ds1;

public class Resource implements Cloneable {
    private Integer value;
    private Integer version;

    public Resource(Integer value, Integer version) {
        this.value = value;
        this.version = version;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    @Override
    public Object clone() {
        return new Resource(value, version);
    }
}
