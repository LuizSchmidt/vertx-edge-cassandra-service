package com.vertx.edge.cassandra.type;

import java.util.Objects;

import io.vertx.cassandra.CassandraClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.spi.ServiceType;

public interface CassandraType extends ServiceType {

  String TYPE = "cassandra";

  static Record createRecord(String name, JsonObject location, JsonObject metadata) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(location);

    Record record = new Record().setName(name).setType(TYPE).setLocation(location);

    if (metadata != null) {
      record.setMetadata(metadata);
    }

    return record;
  }

  static Future<CassandraClient> getCassandraClient(ServiceDiscovery discovery, JsonObject filter) {
    return discovery.getRecord(filter).compose(record -> Future.succeededFuture(discovery.getReference(record).get()));
  }

}
