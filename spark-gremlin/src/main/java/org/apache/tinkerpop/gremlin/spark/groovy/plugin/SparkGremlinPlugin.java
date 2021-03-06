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

package org.apache.tinkerpop.gremlin.spark.groovy.plugin;

import org.apache.spark.metrics.MetricsSystem;
import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SparkGremlinPlugin extends AbstractGremlinPlugin {

    protected static String NAME = "tinkerpop.spark";

    protected static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + SparkGraphComputer.class.getPackage().getName() + DOT_STAR);
    }};

    public SparkGremlinPlugin() {
        super(true);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
        try {
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.INFO)", SparkGraphComputer.class.getName()));
            pluginAcceptor.eval(String.format("Logger.getLogger(%s).setLevel(Level.ERROR)", MetricsSystem.class.getName()));
        } catch (final Exception e) {
            throw new PluginInitializationException(e.getMessage(), e);
        }
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

    @Override
    public Optional<RemoteAcceptor> remoteAcceptor() {
        return Optional.empty();
    }
}