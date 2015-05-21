/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.traversal;

import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserExecutor {

    public static boolean execute(final Vertex vertex, final Messenger<TraverserSet<?>> messenger, final TraversalMatrix<?, ?> traversalMatrix) {

        final TraverserSet<Object> aliveTraversers = new TraverserSet<>();
        final TraverserSet<Object> haltedTraversers = vertex.value(TraversalVertexProgram.HALTED_TRAVERSERS);
        final StepSessions stepSessions = vertex.value(TraversalVertexProgram.SESSION_TRAVERSERS);
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);

        // gather incoming traversers into a traverser set and gain the 'weighted-set' optimization
        final TraversalSideEffects traversalSideEffects = traversalMatrix.getTraversal().getSideEffects();
        messenger.receiveMessages().forEachRemaining(traverserSet -> {
            traverserSet.forEach(traverser -> {
                if (traverser.getSession().isPresent() && traverser.isHalted()) {
                    stepSessions.addSpawn(traverser, traverser.getSession().get().getUUID(), traverser.getSession().get().getLocalTraversalIndex());
                } else {
                    traverser.setSideEffects(traversalSideEffects);
                    traverser.attach(Attachable.Method.get(vertex));
                    aliveTraversers.add((Traverser.Admin) traverser);
                }
            });
        });

        stepSessions.getCompletedSessions().forEach(uuid -> {
            Step<?, ?> step = traversalMatrix.getStepById(stepSessions.getStepId(uuid));
            final Traverser.Admin<?> map = ((PathStep) step).processSpawns(stepSessions.getRoot(uuid), stepSessions.getSpawns(uuid));
            map.killSession();
            aliveTraversers.add((Traverser.Admin) map);
            stepSessions.removeSession(uuid);
        });

        // while there are still local traversers, process them until they leave the vertex or halt (i.e. isHalted()).
        final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
        while (!aliveTraversers.isEmpty()) {
            // process all the local objects and send messages or store locally again
            aliveTraversers.forEach(traverser -> {
                if (traverser.isHalted() && traverser.getSession().isPresent()) {
                    voteToHalt.set(false);
                    traverser.detach();
                    messenger.sendMessage(MessageScope.Global.of(ReferenceVertex.of(traverser.getSession().get().getHostVertexId())), new TraverserSet<>(traverser));
                } else {


                    if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                        // if the element is remote, then message, else store it locally for re-processing
                        final Vertex hostingVertex = TraverserExecutor.getHostingVertex(traverser.get());
                        if (!vertex.equals(hostingVertex)) { // necessary for path access
                            voteToHalt.set(false);
                            traverser.detach();
                            messenger.sendMessage(MessageScope.Global.of(hostingVertex), new TraverserSet<>(traverser));
                        } else {
                            if (traverser.get() instanceof Attachable)   // necessary for path access to local object
                                traverser.attach(Attachable.Method.get(vertex));
                            toProcessTraversers.add(traverser);
                        }
                    } else                                                                              // STANDARD OBJECT
                        toProcessTraversers.add(traverser);
                }
            });

            // process local traversers and if alive, repeat, else halt.
            aliveTraversers.clear();
            toProcessTraversers.forEach(start -> {

                final Step<?, ?> step = traversalMatrix.getStepById(start.getStepId());
                if (step instanceof PathStep)
                    ((PathStep) step).stepSessions = stepSessions;


                step.addStart((Traverser.Admin) start);
                step.forEachRemaining(end -> {
                    if (end.asAdmin().isHalted()) {
                        end.asAdmin().detach();
                        if (end.asAdmin().getSession().isPresent()) {
                            if (!end.asAdmin().getSession().get().getHostVertexId().equals(vertex.id()))
                                aliveTraversers.add((Traverser.Admin) end);
                            else
                                stepSessions.addSpawn((Traverser.Admin) end, end.asAdmin().getSession().get().getUUID(), end.asAdmin().getSession().get().getLocalTraversalIndex());
                        } else
                            haltedTraversers.add((Traverser.Admin) end);
                    } else
                        aliveTraversers.add((Traverser.Admin) end);
                });
                if (step instanceof PathStep) {
                    ((PathStep<?>) step).spawns.forEach(spawn -> aliveTraversers.add((Traverser.Admin) spawn));
                    ((PathStep<?>) step).spawns.clear();
                }

            });

            toProcessTraversers.clear();
        }
        return voteToHalt.get();
    }

    private static Vertex getHostingVertex(final Object object) {
        Object obj = object;
        while (true) {
            if (obj instanceof Vertex)
                return (Vertex) obj;
            else if (obj instanceof Edge)
                return ((Edge) obj).outVertex();
            else if (obj instanceof Property)
                obj = ((Property) obj).element();
            else
                throw new IllegalStateException("The host of the object is unknown: " + obj.toString() + ':' + obj.getClass().getCanonicalName());
        }
    }
}