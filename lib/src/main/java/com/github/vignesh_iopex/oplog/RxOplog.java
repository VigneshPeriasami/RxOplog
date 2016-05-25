package com.github.vignesh_iopex.oplog;

import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.BsonTimestamp;
import org.bson.Document;
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

  private MongoCollection<Document> getOplogCollection() {
    return mongoClient.getDatabase("local").getCollection("oplog.$main");
  }

  private Bson getTimestampQuery() {
    return Filters.gt("ts", new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 0));
  }

  private FindIterable<Document> getOplogCursor() {
    return getOplogCollection().find(getTimestampQuery()).cursorType(CursorType.Tailable);
  }

  public Observable<Document> tail() {
    return Observable.create(new Observable.OnSubscribe<Document>() {
      @Override public void call(final Subscriber<? super Document> observer) {
        getOplogCursor().forEach(new Block<Document>() {
          @Override public void apply(Document doc) {
            observer.onNext(doc);
          }
        });
        observer.onCompleted();
      }
    });
  }
}
