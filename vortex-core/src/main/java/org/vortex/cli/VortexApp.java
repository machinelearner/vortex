package org.vortex.cli;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.ConsistentHashingRoutingLogic;
import akka.routing.Router;
import org.vortex.actors.TargetActor;

public class VortexApp {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("VortexApp");
        ActorRef a = system.actorOf(Props.create(TargetActor.class), TargetActor.class.getSimpleName());
        new Router(new ConsistentHashingRoutingLogic(system));
    }
}
