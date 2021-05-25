package com.vertx.edge.cassandra.reference;

import io.vertx.cassandra.CassandraClient;
import io.vertx.cassandra.CassandraClientOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.AbstractServiceReference;

public class CassandraServiceReference extends AbstractServiceReference<CassandraClient> {

  private final JsonObject config;

  public CassandraServiceReference(Vertx vertx, ServiceDiscovery discovery, Record record, JsonObject config) {
    super(vertx, discovery, record);
    this.config = config;
  }

  @Override
  public CassandraClient retrieve() {
    JsonObject result = record().getMetadata().copy();
    result.mergeIn(record().getLocation());

    if (config != null) {
      result.mergeIn(config);
    }

    CassandraClientOptions options = new CassandraClientOptions(config);
    if (result.getBoolean("shared", Boolean.FALSE).booleanValue()) {
      return CassandraClient.createShared(vertx, options);
    } else {
      return CassandraClient.create(vertx, options);
    }
  }

  @Override
  protected void onClose() {
    service.close();
  }
}
