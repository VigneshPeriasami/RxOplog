package com.github.vignesh_iopex.oplog;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import rx.Observable;
import rx.Subscriber;

public class RxOplog {
  private final MongoClient mongoClient;

  public RxOplog(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  public static RxOplog connect(String host, int port) {
    return new RxOplog(new MongoClient(host, port));
  }

  private MongoCollection<Document> getOplogCollection(String collectionName) {
    return mongoClient.getDatabase("local").getCollection(collectionName);
  }

  private MongoCursor<Document> getOplogIterable(CursorOptions cursorOptions) {
    return getOplogCollection(cursorOptions.getCollectionName())
        .find(cursorOptions.getOpQuery()).cursorType(CursorType.Tailable).iterator();
  }

  public Observable<Document> tail() {
    return tail(CursorOptions.DEFAULT);
  }

  public Observable<Document> tail(final CursorOptions cursorOptions) {
    return Observable.create(new Observable.OnSubscribe<Document>() {
      @Override public void call(final Subscriber<? super Document> observer) {
        MongoCursor<Document> iterator = getOplogIterable(cursorOptions);
        while (iterator.hasNext()) {
          if (observer.isUnsubscribed()) {
            break;
          }
          observer.onNext(iterator.next());
        }
        iterator.close();
        observer.onCompleted();
      }
    });
  }
}
