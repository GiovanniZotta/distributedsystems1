package it.unitn.ds1.resources;

public class WorkspaceResource extends Resource {

    Boolean changed;

    public WorkspaceResource(Resource resource, Boolean changed) {
        super(resource.getValue(), resource.getVersion());
        this.changed = changed;
    }

    public Boolean getChanged() {
        return changed;
    }

    public void setChanged(Boolean changed) {
        this.changed = changed;
    }
}