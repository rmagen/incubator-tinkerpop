/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.computer.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StepSessions implements Serializable {

    private final Object hostVertexId;
    private final Map<UUID, Traverser.Admin<?>> roots = new HashMap<>();
    private final Map<UUID, Integer> counters = new HashMap<>();
    private final Map<UUID, Map<Integer, TraverserSet<?>>> spawns = new HashMap<>();
    private final Map<UUID, String> stepId = new HashMap<>();

    public StepSessions(final Vertex hostVertex) {
        this.hostVertexId = hostVertex.id();

    }

    public Object getHostVertexId() {
        return this.hostVertexId;
    }

    public boolean hasSession(final UUID uuid) {
        return this.counters.containsKey(uuid);
    }

    public <S> Traverser.Admin<S> getRoot(final UUID uuid) {
        return (Traverser.Admin<S>) this.roots.get(uuid);
    }

    public void addSpawn(final Traverser.Admin<?> traverser, final UUID uuid, final int traversalIndex) {

        TraverserSet traverserSet = this.spawns.get(uuid).get(traversalIndex);
        if (null == traverserSet) {
            traverserSet = new TraverserSet<>();
            this.spawns.get(uuid).put(traversalIndex, traverserSet);
        }
        traverserSet.add(traverser);
    }

    public <S> Map<Integer, TraverserSet<S>> getSpawns(final UUID uuid) {
        return (Map) this.spawns.get(uuid);
    }

    public String getStepId(final UUID uuid) {
        return this.stepId.get(uuid);
    }

    public void saveRootAndSpawns(final UUID uuid, final Traverser.Admin<?> root, final TraversalParent step) {
        this.roots.put(uuid, root);
        this.counters.put(uuid, step.getLocalChildren().size());
        this.spawns.put(uuid, new HashMap<>());
        this.stepId.put(uuid, step.asStep().getId());
    }

    public List<UUID> getCompletedSessions() {
        return this.roots.keySet().stream().filter(uuid -> spawns.get(uuid).size() == counters.get(uuid)).collect(Collectors.toList());
    }

    public void removeSession(final UUID uuid) {
        this.roots.remove(uuid);
        this.counters.remove(uuid);
        this.spawns.remove(uuid);
        this.stepId.remove(uuid);
    }
}
