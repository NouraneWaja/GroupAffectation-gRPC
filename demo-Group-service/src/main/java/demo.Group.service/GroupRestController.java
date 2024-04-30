package demo.Group.service;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.Descriptors.FieldDescriptor;

@RestController
public class GroupRestController {
	
	@Autowired
	private GroupGrpcClient GroupGrpcClient;

	
	@RequestMapping(path = "/Group/{GroupID}/Assignment")
	public List<Map<FieldDescriptor, Object>> getAssignmentByGroupID(@PathVariable Long GroupID,
																	 @RequestParam(value = "isSyncClient", required = false, defaultValue = "Y")
																	 String isSyncClient)
			throws InterruptedException {
		
		return isSyncClient.equals("Y") ? GroupGrpcClient.getAssignmentByGroupIDViaSynchronousClient(GroupID)
				: GroupGrpcClient.getAssignmentByGroupIDViaAsynchronousClient(GroupID);
	}
	
}
