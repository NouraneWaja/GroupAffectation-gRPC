package demo.Group.service;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.stereotype.Service;

import com.google.protobuf.Descriptors.FieldDescriptor;

import demo.interfaces.grpc.Assignment;
import demo.interfaces.grpc.AssignmentServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;

@Service
public class GroupGrpcClient {

	@GrpcClient("Assignment-service")
	private AssignmentServiceGrpc.AssignmentServiceBlockingStub AssignmentServiceBlockingStub;
	
	@GrpcClient("Assignment-service")
	private AssignmentServiceGrpc.AssignmentServiceStub AssignmentServiceStub;

	/*
	 * Synchronous client implementation
	 * 
	 * Client side implementation of Server streaming RPCs where the
	 * client sends a request to the server and gets a stream to read a sequence of
	 * messages back. The client reads from the returned stream until there are no
	 * more messages. gRPC guarantees message ordering within an individual RPC
	 * call
	 */
	public List<Map<FieldDescriptor, Object>> getAssignmentByGroupIDViaSynchronousClient(long GroupID) {

		Iterator<Assignment> AssignmentReply = AssignmentServiceBlockingStub
				.getAssignmentByGroup(Assignment.newBuilder().setGroupID(GroupID).build());

		//Convert Iterator to stream Stream<Assignment>
		Iterable<Assignment> iterable = () -> AssignmentReply;
		Stream<Assignment> AssignmentStream = StreamSupport.stream(iterable.spliterator(), false);

		return AssignmentStream.map(Assignment -> Assignment.getAllFields()).collect(Collectors.toList());

	}
	
	/*
	 * Asynchronous client implementation
	 * Client side implementation of Server streaming RPCs where the
	 * client sends a request to the server and gets a stream to read a sequence of
	 * messages back. The client reads from the returned stream until there are no
	 * more messages. gRPC guarantees message ordering within an individual RPC
	 * call.
	 */
	public List<Map<FieldDescriptor, Object>> getAssignmentByGroupIDViaAsynchronousClient(long GroupID) throws InterruptedException {
		
		final CountDownLatch finishLatch = new CountDownLatch(1);
		List<Map<FieldDescriptor, Object>> responceList = new ArrayList<Map<FieldDescriptor,Object>>();
		
		AssignmentServiceStub.getAssignmentByGroup(Assignment.newBuilder().setGroupID(GroupID).build(), new StreamObserver<Assignment>() {
			
			@Override
			public void onNext(Assignment value) {
				responceList.add(value.getAllFields());				
			}
			
			@Override
			public void onError(Throwable t) {
				finishLatch.countDown();
				
			}
			
			@Override
			public void onCompleted() {
				finishLatch.countDown();
				
			}
		});
		
		finishLatch.await(1, TimeUnit.MINUTES);

		return responceList;

	}
}
