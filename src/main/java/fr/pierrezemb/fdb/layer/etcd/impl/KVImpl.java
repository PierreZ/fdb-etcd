package fr.pierrezemb.fdb.layer.etcd.impl;


import fr.pierrezemb.etcd.pb.KVGrpc;
import fr.pierrezemb.etcd.pb.Rpc;
import io.vertx.core.Promise;

public class KVImpl extends KVGrpc.KVVertxImplBase {

  /**
   * <pre>
   * Range gets the keys in the range from the key-value store.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void range(Rpc.RangeRequest request, Promise<Rpc.RangeResponse> response) {
    super.range(request, response);
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
  public void put(Rpc.PutRequest request, Promise<Rpc.PutResponse> response) {
    super.put(request, response);
  }

  /**
   * <pre>
   * DeleteRange deletes the given range from the key-value store.
   * A delete request increments the revision of the key-value store
   * and generates a delete event in the event history for every deleted key.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void deleteRange(Rpc.DeleteRangeRequest request, Promise<Rpc.DeleteRangeResponse> response) {
    super.deleteRange(request, response);
  }

  /**
   * <pre>
   * Txn processes multiple requests in a single transaction.
   * A txn request increments the revision of the key-value store
   * and generates events with the same revision for every completed request.
   * It is not allowed to modify the same key several times within one txn.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void txn(Rpc.TxnRequest request, Promise<Rpc.TxnResponse> response) {
    super.txn(request, response);
  }

  /**
   * <pre>
   * Compact compacts the event history in the etcd key-value store. The key-value
   * store should be periodically compacted or the event history will continue to grow
   * indefinitely.
   * </pre>
   *
   * @param request
   * @param response
   */
  @Override
  public void compact(Rpc.CompactionRequest request, Promise<Rpc.CompactionResponse> response) {
    super.compact(request, response);
  }
}
