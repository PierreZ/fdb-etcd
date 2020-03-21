package fr.pierrezemb.fdb.layer.etcd.impl;


import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import etcdserverpb.EtcdIoRpcProto;
import etcdserverpb.KVGrpc;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;
import fr.pierrezemb.fdb.layer.etcd.EtcdRecordStore;
import io.vertx.core.Promise;

public class KVImpl extends KVGrpc.KVVertxImplBase {

  private EtcdRecordStore recordStore;

  public KVImpl(EtcdRecordStore recordStore) {
    this.recordStore = recordStore;
  }

  /**
   * <pre>
   * Put puts the given key into the key-value store.
   * A put request increments the revision of the key-value store
   * and generates one event in the event history.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void put(EtcdIoRpcProto.PutRequest request, Promise<EtcdIoRpcProto.PutResponse> response) {
    System.out.println("inside put");
    this.recordStore.db.run(context -> {
      FDBRecordStore recordStore = this.recordStore.recordStoreProvider.apply(context);
      recordStore.saveRecord(EtcdRecord.PutRequest.newBuilder()
        .setKey(request.getKey())
        .setValue(request.getValue())
        .setLease(request.getLease())
        .setPrevKv(request.getPrevKv())
        .setIgnoreValue(request.getIgnoreValue())
        .setIgnoreLease(request.getIgnoreLease()).build());
      System.out.println("save done");
      return null;
    });
    response.complete();
  }
}
