package fr.pierrezemb.fdb.layer.etcd.utils;

import com.google.protobuf.InvalidProtocolBufferException;
import etcdserverpb.EtcdIoRpcProto;
import fr.pierrezemb.etcd.record.pb.EtcdRecord;

public class ProtoUtils {
  public static EtcdRecord.PutRequest from(EtcdIoRpcProto.PutRequest request) throws InvalidProtocolBufferException {
    return EtcdRecord.PutRequest.parseFrom(request.toByteArray());
  }
}
