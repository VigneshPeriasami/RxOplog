package com.github.vignesh_iopex.oplog;

import org.bson.Document;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

public class Runner {

  /** Filter non empty name space and process document to return Json string */
  public static Observable<String> applyOperators(Observable<Document> observable) {
    return observable.filter(new Func1<Document, Boolean>() {
      @Override public Boolean call(Document document) {
        return !document.getString("ns").isEmpty();
      }
    }).map(new Func1<Document, String>() {
      @Override public String call(Document o) {
        return o.toJson();
      }
    });
  }

  public static void main(String[] args) {
    applyOperators(RxOplog.create("localhost", 27017).tail())
        .subscribe(new Subscriber<String>() {
          @Override public void onCompleted() {
            System.out.println("Job listening to oplog is terminated");
          }

          @Override public void onError(Throwable e) {
            e.printStackTrace();
          }

          @Override public void onNext(String s) {
            System.out.println(s);
          }
        });
  }
}
