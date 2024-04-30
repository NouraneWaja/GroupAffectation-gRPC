package demo.Assignment.service;
import java.util.ArrayList;
import java.util.List;

import demo.interfaces.grpc.Assignment;

public class AssignmentResourceProvider {

	public static List<Assignment> getAssignmentfromAssignmentSource() {
		return new ArrayList<Assignment>() {
			{
				add(Assignment.newBuilder()
						.setAssignmentID(1)
						.setGroupID(1)
						.setProjectID(1)
						.setDescription(" Le groupe 1 du GL-04 est dédié a un projet GRPC")
						.build());

				add(Assignment.newBuilder()
						.setAssignmentID(2)
						.setGroupID(2)
						.setProjectID(1)
						.setDescription("Le groupe 2 du GL-04 est dédié a un projet GRPC")
						.build());

				add(Assignment.newBuilder()
						.setAssignmentID(3)
						.setGroupID(3)
						.setProjectID(1)
						.setDescription("Le groupe 3 du GL-04 est dédié a un projet GRPC")
						.build());

				add(Assignment.newBuilder()
						.setAssignmentID(4)
						.setGroupID(4)
						.setProjectID(2)
						.setDescription("Le groupe 1 du GL-02 est dédié a un projet AOP")
						.build());
				add(Assignment.newBuilder()
						.setAssignmentID(5)
						.setGroupID(5)
						.setProjectID(3)
						.setDescription("Le groupe 2 du GL-02 est dédié a un projet WebFlux")
						.build());



			}
		};
	}
}
