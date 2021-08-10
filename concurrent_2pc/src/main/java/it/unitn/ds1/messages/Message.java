package it.unitn.ds1.messages;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Message {

    // send this message to the client at startup to inform it about the group and the keys
    public static class WelcomeMsg implements Serializable {
        public final Integer maxKey;
        public final List<ActorRef> group;
        public WelcomeMsg(int maxKey, List<ActorRef> group) {
            this.maxKey = maxKey;
            this.group = Collections.unmodifiableList(new ArrayList<>(group));
        }
    }
}
