package com.github.vignesh_iopex.oplog;

import rx.functions.Action1;

public class Runner {
  public static void main(String[] args) {
    RxOplog.create("localhost", 27017).tail().subscribe(new Action1() {
      @Override public void call(Object o) {
        System.out.println(o);
      }
    });
  }
}
