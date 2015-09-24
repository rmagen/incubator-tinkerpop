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

package org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.io.gryo;

import org.apache.spark.SerializableWritable;
import org.apache.spark.api.python.PythonBroadcast;
import org.apache.spark.broadcast.HttpBroadcast;
import org.apache.spark.scheduler.CompressedMapStatus;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.payload.MessagePayload;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.payload.ViewIncomingPayload;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.hadoop.process.computer.spark.payload.ViewPayload;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.shaded.kryo.serializers.JavaSerializer;
import scala.Tuple2;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GryoSerializer extends Serializer {
    @Override
    public SerializerInstance newInstance() {
        return new GryoSerializerInstance(
                GryoMapper.build().
                        addCustom(SerializableWritable.class, new JavaSerializer()).
                        addCustom(Tuple2.class, new JavaSerializer()).
                        addCustom(CompressedMapStatus.class, new JavaSerializer()).
                        addCustom(HttpBroadcast.class, new JavaSerializer()).
                        addCustom(PythonBroadcast.class, new JavaSerializer()).
                        addCustom(MessagePayload.class, new JavaSerializer()).
                        addCustom(ViewIncomingPayload.class, new JavaSerializer()).
                        addCustom(ViewOutgoingPayload.class, new JavaSerializer()).
                        addCustom(ViewPayload.class, new JavaSerializer()).
                        create().createMapper());
        // kryo.register(org.apache.spark.serializer.JavaIterableWrapperSerializer..MODULE$.wrapperClass(), new JavaIterableWrapperSerializer());
    }
}
