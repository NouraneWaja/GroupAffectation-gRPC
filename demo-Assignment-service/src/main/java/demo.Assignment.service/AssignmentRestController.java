package demo.Assignment.service;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.Descriptors.FieldDescriptor;

@RestController
public class AssignmentRestController {

	@Autowired
	private AssignmentGrpcClientImpl AssignmentGrpcClientImpl;

	
	@GetMapping("/Assignment/{AssignmentID}")
	public Map<FieldDescriptor, Object> getGroupDetailsByAssignmentID(@PathVariable Long AssignmentID) {
		return AssignmentGrpcClientImpl.getGroupDetailsByAssignmentID(AssignmentID);
	}

	@GetMapping("/Assignment")
	public List<Map<FieldDescriptor, Object>> getFullGroupListByProjectID(@RequestParam(value = "projectID", required = true) Long projectID)
			throws InterruptedException {
		return AssignmentGrpcClientImpl.getGroupFullDetails(projectID);
	}
	
	@GetMapping("{projectID}/Assignment/getLargestGroupInProject")
	public Map<String, Map<FieldDescriptor, Object>> getLargestGroup(@PathVariable Long projectID) throws InterruptedException {
		return AssignmentGrpcClientImpl.getLargestGroup(projectID);
	}

}
