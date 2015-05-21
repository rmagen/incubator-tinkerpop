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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.traversal.StepSessions;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathStep<S> extends MapStep<S, Path> implements TraversalParent, EngineDependent {

    private TraversalRing<Object, Object> traversalRing;
    private boolean onComputer = false;
    public StepSessions stepSessions;
    public List<Traverser.Admin<?>> spawns = new ArrayList<>();

    public PathStep(final Traversal.Admin traversal) {
        super(traversal);
        this.traversalRing = new TraversalRing<>();
    }

    @Override
    protected Path map(final Traverser.Admin<S> traverser) {
        final Path path;
        if (this.traversalRing.isEmpty())
            path = traverser.path();
        else {
            if (this.onComputer) {
                this.doOnComputer(traverser);
                throw FastNoSuchElementException.instance();
            } else {
                path = MutablePath.make();
                traverser.path().forEach((object, labels) -> path.extend(TraversalUtil.apply(object, this.traversalRing.next()), labels));
            }
        }
        this.traversalRing.reset();
        return path;
    }

    private void doOnComputer(final Traverser.Admin<S> root) {
        final UUID uuid = UUID.randomUUID();
         spawns = new ArrayList<>();
        root.path().forEach((object, labels) -> {
            final Traverser.Admin<?> split = root.split(object, (Step) this);
            split.setStepId(this.traversalRing.next().getStartStep().getId());
            split.setSession(new Traverser.Admin.Session(uuid, stepSessions.getHostVertexId(), this.traversalRing.getCurrentTraversal()));
            spawns.add(split);
        });
        this.stepSessions.saveRootAndSpawns(uuid, root, this);
    }

    public Traverser.Admin<Path> processSpawns(final Traverser.Admin<S> root, final Map<Integer, TraverserSet<?>> spawns) {
        Path path = MutablePath.make();
        for (int i = 0; i < root.path().size(); i++) {
            path.extend(spawns.get(i).iterator().next(), root.path().labels().get(i));
        }
        final Traverser.Admin<Path> split = root.split(path, this);
        split.setStepId(this.getNextStep().getId());
        split.killSession();

        return split;
    }

    @Override
    public PathStep<S> clone() {
        final PathStep<S> clone = (PathStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        clone.getLocalChildren().forEach(clone::integrateChild);
        clone.spawns = new ArrayList<>();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> pathTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(pathTraversal));
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.traversalRing);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH);
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.onComputer = traversalEngine.isComputer();
    }
}
