package org.vortex.actors;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class TargetActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
        ActorRef actorRef = getContext().system().actorFor("/");
        actorRef.tell(message, this.getSelf());
    }
}
