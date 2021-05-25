/*
 * Vert.x Edge, open source.
 * Copyright (C) 2020-2021 Vert.x Edge
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.vertx.edge.cassandra;

import com.vertx.edge.annotations.ServiceProvider;
import com.vertx.edge.cassandra.type.CassandraType;
import com.vertx.edge.deploy.service.RecordService;
import com.vertx.edge.deploy.service.secret.Secret;

import io.vertx.cassandra.CassandraClient;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;

/**
 * @author Luiz Schmidt
 */
@ServiceProvider(name = CassandraService.SERVICE)
public class CassandraService implements RecordService {

  public static final String SERVICE = "cassandra-service";

  public static Future<CassandraClient> cassandra(ServiceDiscovery discovery) {
    Promise<CassandraClient> promise = Promise.promise();

    CassandraType.getCassandraClient(discovery, new JsonObject().put("name", SERVICE)).onSuccess(promise::complete)
        .onFailure(cause -> promise.fail(RecordService.buildErrorMessage(SERVICE, cause)));

    return promise.future();
  }

  public Future<Record> newRecord(Vertx vertx, JsonObject config) {
    Promise<Record> promise = Promise.promise();
    buildCassandraOptions(vertx, config)
        .onSuccess(opts -> promise.complete(CassandraType.createRecord(SERVICE, new JsonObject(), opts)))
        .onFailure(promise::fail);
    return promise.future();
  }

  private static Future<JsonObject> buildCassandraOptions(Vertx vertx, JsonObject config) {
    return Secret.getUsernameAndPassword(vertx, config).compose(v -> Future.succeededFuture(config.mergeIn(v)));
  }
}
