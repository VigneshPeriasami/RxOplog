package com.github.vignesh_iopex.oplog;

import com.mongodb.client.model.Filters;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;

public interface CursorOptions {
  String getCollectionName();

  Bson getOpQuery();

  CursorOptions DEFAULT = new CursorOptions() {
    @Override public String getCollectionName() {
      return "oplog.$main";
    }

    @Override public Bson getOpQuery() {
      return Filters.gt("ts", new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0));
    }
  };
}
