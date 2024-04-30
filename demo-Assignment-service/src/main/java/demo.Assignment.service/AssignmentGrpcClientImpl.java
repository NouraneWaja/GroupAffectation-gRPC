package demo.Assignment.service;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import com.google.protobuf.Descriptors.FieldDescriptor;

import demo.interfaces.grpc.Group;
import demo.interfaces.grpc.GroupServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;

@Service
public class AssignmentGrpcClientImpl {

	@GrpcClient("Group-service")
	private GroupServiceGrpc.GroupServiceBlockingStub GroupServiceBlockingStub;

	@GrpcClient("Group-service")
	private GroupServiceGrpc.GroupServiceStub GroupServiceStub;

	/**
	 * Client side implementation of unary RPCs where the client sends a single request
	 * to the server and gets a single response back, just like a normal function
	 * call.
	 * 
	 * @param AssignmentID
	 * @return Group Object Map<FieldDescriptor, Object>
	 */
	public Map<FieldDescriptor, Object> getGroupDetailsByAssignmentID(long AssignmentID) {

		return GroupServiceBlockingStub
					.getGroup(Group.newBuilder()
											.setGroupID(AssignmentResourceProvider
															.getAssignmentfromAssignmentSource()
															.stream()
															.filter(alloc -> alloc.getAssignmentID() == AssignmentID)
															.findFirst()
															.get()
															.getGroupID())
											.build())
					.getAllFields();
	}

	/*
	 * Client side implementation of bidirectional streaming RPCs where both sides
	 * send a sequence of messages using a read-write stream. The two streams
	 * operate independently, so clients and servers can read and write in whatever
	 * order they like: for example, the server could wait to receive all the client
	 * messages before writing its responses, or it could alternately read a message
	 * then write a message, or some other combination of reads and writes. The
	 * order of messages in each stream is preserved.
	 * 
	 */
	public List<Map<FieldDescriptor, Object>> getGroupFullDetails(long projectID) throws InterruptedException {

		final CountDownLatch finishLatch = new CountDownLatch(1);
		List<Map<FieldDescriptor, Object>> GroupDetailsFinalList = new ArrayList<Map<FieldDescriptor, Object>>();

		StreamObserver<Group> responseObserver = GroupServiceStub
				.getAllGroupsByIDList(new StreamObserver<Group>() {
					@Override
					public void onNext(Group value) {
						GroupDetailsFinalList.add(value.getAllFields());
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

		AssignmentResourceProvider.getAssignmentfromAssignmentSource().stream()
				.filter(alloc -> alloc.getProjectID() == projectID).forEach(alloc -> {
					responseObserver.onNext(Group.newBuilder().setGroupID(alloc.getGroupID()).build());
				});

		responseObserver.onCompleted();

		finishLatch.await(1, TimeUnit.MINUTES);

		return GroupDetailsFinalList;
	}

	/**
	 * Client side implementation of Client streaming RPCs where the client writes a
	 * sequence of messages and sends them to the server, again using a provided
	 * stream. Once the client has finished writing the messages, it waits for the
	 * server to read them and return its response. Again gRPC guarantees message
	 * ordering within an individual RPC call.
	 * 
	 * @param projectID
	 * @return
	 * @throws InterruptedException
	 */
	public Map<String, Map<FieldDescriptor, Object>> getLargestGroup(long projectID) throws InterruptedException {

		final CountDownLatch finishLatch = new CountDownLatch(1);
		Map<String, Map<FieldDescriptor, Object>> responce = new HashMap<String, Map<FieldDescriptor, Object>>();
		

		StreamObserver<Group> responseObserver = GroupServiceStub
				.getLargestGroup(new StreamObserver<Group>() {
								
					@Override
					public void onNext(Group value) {
						responce.put("Group", value.getAllFields());
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

		AssignmentResourceProvider.getAssignmentfromAssignmentSource().stream()
				.filter(alloc -> alloc.getProjectID() == projectID).forEach(alloc -> {
					responseObserver.onNext(Group.newBuilder().setGroupID(alloc.getGroupID()).build());
				});

		responseObserver.onCompleted();

		finishLatch.await(1, TimeUnit.MINUTES);

		return responce;
	}

}
