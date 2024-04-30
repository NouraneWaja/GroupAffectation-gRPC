package demo.Group.service;
import java.util.ArrayList;
import java.util.List;

import demo.interfaces.grpc.Group;
import demo.interfaces.grpc.GroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class GroupGrpcServiceImpl extends GroupServiceGrpc.GroupServiceImplBase {
	
	/*
	 * Server side implementation of unary RPCs where the client sends a single request
	 * to the server and gets a single response back, just like a normal function
	 * call.
	 for understanding
	 * demo.interfaces.grpc.GroupServiceGrpc.GroupServiceImplBase#getGroup(
	 * demo.interfaces.grpc.GroupRequest, io.grpc.stub.StreamObserver)
	 */
	@Override
	public void getGroup(Group request, StreamObserver<Group> responseObserver) {
		responseObserver.onNext(GroupResourceProvider.getGroupListfromGroupSource()
													.stream()
													.filter(emp -> emp.getGroupID() == request.getGroupID())
													.findFirst()
													.get());
		responseObserver.onCompleted();
	}

	/*
	 * Server side implementation of bidirectional streaming RPCs where both sides
	 * send a sequence of messages using a read-write stream. The two streams
	 * operate independently, so clients and servers can read and write in whatever
	 * order they like: for example, the server could wait to receive all the client
	 * messages before writing its responses, or it could alternately read a message
	 * then write a message, or some other combination of reads and writes. The
	 * order of messages in each stream is preserved.
     for understanding
	 *  demo.interfaces.grpc.GroupServiceGrpc.GroupServiceImplBase#
	 * getAllGroupsByName(io.grpc.stub.StreamObserver)
	 */
	@Override
	public StreamObserver<Group> getAllGroupsByIDList(StreamObserver<Group> responseObserver) {
		return new StreamObserver<Group>() {
			
			List<Group> responseList = new ArrayList<Group>();
			
			@Override
			public void onNext(Group value) {
				responseList.add(GroupResourceProvider.getGroupListfromGroupSource().stream()
										.filter(emp -> emp.getGroupID() == value.getGroupID())
										.findFirst()
										.get());
			}
			
			@Override
			public void onError(Throwable t) {
				responseObserver.onError(t);				
			}
			
			@Override
			public void onCompleted() {
				responseList.stream().forEach(responseObserver::onNext);
				responseObserver.onCompleted();
			}
		};
	}

	/*
	 * Server side implementation of Client streaming RPCs where the client writes a
	 * sequence of messages and sends them to the server, again using a provided
	 * stream. Once the client has finished writing the messages, it waits for the
	 * server to read them and return its response. Again gRPC guarantees message
	 * ordering within an individual RPC call.
	 for understanding
	 * demo.interfaces.grpc.GroupServiceGrpc.GroupServiceImplBase#
	 * getLargestGroup(io.grpc.stub.StreamObserver)
	 */
	@Override
	public StreamObserver<Group> getLargestGroup(StreamObserver<Group> responseObserver) {
		return new StreamObserver<Group>() {

			Group response = null;

			@Override
			public void onNext(Group value) {
				
				Group currentGroup = GroupResourceProvider.getGroupListfromGroupSource()
											.stream()
											.filter(emp -> emp.getGroupID() == value.getGroupID())
											.findFirst()
											.get();
																						
				if(response == null || currentGroup.getGroupNumberMembers() > response.getGroupNumberMembers()) {
					response = currentGroup;
				}
			}

			@Override
			public void onError(Throwable t) {
				responseObserver.onError(t);
			}

			@Override
			public void onCompleted() {
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			}
		};
	}	
	
}
