package demo.Assignment.service;
import demo.interfaces.grpc.Assignment;
import demo.interfaces.grpc.AssignmentServiceGrpc;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService
public class AssignmentGrpcServiceImpl extends AssignmentServiceGrpc.AssignmentServiceImplBase {

	/*
	 * Server side implementation of Server streaming RPCs where the
	 * client sends a request to the server and gets a stream to read a sequence of
	 * messages back. The client reads from the returned stream until there are no
	 * more messages. gRPC guarantees message ordering within an individual RPC
	 * call.
	 * 
	 * @see demo.interfaces.grpc.AssignmentServiceGrpc.AssignmentServiceImplBase#
	 * getAssignmentByGroup(demo.interfaces.grpc.
	 * AssignmentRequestForGetAssignmentByEmp, io.grpc.stub.StreamObserver)
	 */
	@Override
	public void getAssignmentByGroup(Assignment request, StreamObserver<Assignment> responseObserver) {
		
		AssignmentResourceProvider.getAssignmentfromAssignmentSource().stream()
				.filter(alloc -> alloc.getGroupID() == request.getGroupID())
				.forEach(responseObserver::onNext);

		responseObserver.onCompleted();
	}	

}
