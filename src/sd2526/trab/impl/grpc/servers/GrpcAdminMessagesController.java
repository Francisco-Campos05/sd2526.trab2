package sd2526.trab.impl.grpc.servers;

import static sd2526.trab.impl.grpc.common.DataModelAdaptor.GrpcAdminMessage_to_Message;

import com.google.protobuf.Empty;

import io.grpc.ServerServiceDefinition;
import io.grpc.stub.StreamObserver;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf.GrpcAdminMessage;
import sd2526.trab.impl.grpc.generated_java.AdminMessagesProtoBuf.RemoteDeleteMessageArgs;
import sd2526.trab.impl.grpc.generated_java.GrpcAdminMessagesGrpc;
import sd2526.trab.impl.java.servers.JavaMessages;

public class GrpcAdminMessagesController extends GrpcController implements GrpcAdminMessagesGrpc.AsyncService {

    AdminMessages impl = JavaMessages.getInstance();

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcAdminMessagesGrpc.bindService(this);
    }

    @Override
    public void remotePostMessage(GrpcAdminMessage request, StreamObserver<Empty> responseObserver) {
        super.toGrpcResult(responseObserver,
                ((AdminMessages) impl).remotePostMessage(GrpcAdminMessage_to_Message(request)),
                (__) -> Empty.newBuilder().build());
    }

    @Override
    public void remoteDeleteMessage(RemoteDeleteMessageArgs request, StreamObserver<Empty> responseObserver) {
        super.toGrpcResult(responseObserver,
                ((AdminMessages) impl).remoteDeleteMessage(request.getMid()),
                (__) -> Empty.newBuilder().build());
    }

    @Override
    public void remoteDeleteUserInbox(AdminMessagesProtoBuf.RemoteDeleteUserInboxArgs request, StreamObserver<Empty> responseObserver) {
        super.toGrpcResult(responseObserver,
                ((AdminMessages) impl).remoteDeleteUserInbox(request.getName()),
                (__) -> Empty.newBuilder().build());
    }
}
