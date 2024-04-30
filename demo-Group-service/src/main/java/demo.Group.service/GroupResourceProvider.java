package demo.Group.service;

import java.util.ArrayList;
import java.util.List;

import demo.interfaces.grpc.Group;

public class GroupResourceProvider {

	public static List<Group> getGroupListfromGroupSource() {
		return new ArrayList<Group>() {
			{
				add(Group.newBuilder()
						.setGroupID(1)
						.setGroupParent("GL-G04")
						.setGroupName("Groupe 1")
						.setGroupNumberMembers(4)
						.build());
				add(Group.newBuilder()
						.setGroupID(2)
						.setGroupParent("GL-G04")
						.setGroupName("Groupe 2")
						.setGroupNumberMembers(3)
						.build());
				add(Group.newBuilder()
						.setGroupID(3)
						.setGroupParent("GL-G04")
						.setGroupName("Groupe 3")
						.setGroupNumberMembers(2)
						.build());
				add(Group.newBuilder()
						.setGroupID(4)
						.setGroupParent("GL-G02")
						.setGroupName("Groupe 1")
						.setGroupNumberMembers(6)
						.build());
				add(Group.newBuilder()
						.setGroupID(5)
						.setGroupParent("GL-G02")
						.setGroupName("Groupe 2")
						.setGroupNumberMembers(5)
						.build());


			}
		};
	}
}
