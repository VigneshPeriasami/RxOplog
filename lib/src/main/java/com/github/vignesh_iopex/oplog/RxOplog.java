package com.github.vignesh_iopex.oplog;

import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;
import rx.Observable;
import rx.Subscriber;

public class RxOplog {
  private final MongoClient mongoClient;

  public RxOplog(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public static RxOplog create(String host, int port) {
    return new RxOplog(new MongoClient(host, port));
  }

  private MongoCollection getOplogCollection() {
    return mongoClient.getDatabase("local").getCollection("oplog.$main");
  }

  private Bson getTimestampQuery() {
    return Filters.gt("ts", new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0));
  }

  private FindIterable getOplogCursor() {
    return getOplogCollection().find(getTimestampQuery()).cursorType(CursorType.Tailable);
  }

  @SuppressWarnings("unchecked")
  public Observable tail() {
    return Observable.create(new Observable.OnSubscribe<Object>() {
      @Override public void call(final Subscriber<? super Object> observer) {
        getOplogCursor().forEach(new Block() {
          @Override public void apply(Object o) {
            observer.onNext(o);
          }
        });
        observer.onCompleted();
      }
    });
  }
}
