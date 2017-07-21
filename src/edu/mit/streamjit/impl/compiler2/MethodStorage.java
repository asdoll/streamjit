package edu.mit.streamjit.impl.compiler2;

import java.lang.invoke.MethodHandle;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class MethodStorage {
	MethodHandle code;
	ActorGroup actorGroup;
	
	public MethodStorage(MethodHandle code, ActorGroup actorGroup){
		this.code = code;
		this.actorGroup = actorGroup;
	}
	
	public Set<Storage> inputs(){
		return actorGroup.inputs();
	}
	
	public Set<Storage> outputs(){
		return actorGroup.outputs();
	}
	
	public boolean read(){
		
	}
}
