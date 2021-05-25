package com.vertx.edge.cassandra.type;

import com.vertx.edge.cassandra.reference.CassandraServiceReference;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceReference;

public class CassandraTypeImpl implements CassandraType {

  @Override
  public String name() {
    return CassandraType.TYPE;
  }

  @Override
  public ServiceReference get(Vertx vertx, ServiceDiscovery discovery, Record record, JsonObject configuration) {
    return new CassandraServiceReference(vertx, discovery, record, configuration);
  }

}
