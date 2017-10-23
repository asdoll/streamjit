ackage edu.mit.streamjit.impl.compiler2;

import java.lang.invoke.MethodHandle;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.mit.streamjit.api.Rate;

public class MethodStorage {
	MethodHandle code;
	ActorGroup actorGroup;
	ImmutableMap<Storage, ConcreteStorage> storage;
	
	public MethodStorage(MethodHandle code, ActorGroup actorGroup,ImmutableMap<Storage, ConcreteStorage> storage){
		this.code = code;
		this.actorGroup = actorGroup;
		this.storage =storage;
	}
	
	public Set<Storage> inputs(){
		return actorGroup.inputs();
	}
	
	public Storage input(int i){
		int t = 0;
		for(Storage input:actorGroup.inputs()){
			if(t == i) return input;
			t++;
			}
		return null;
	}
	
	public Set<Storage> outputs(){
		return actorGroup.outputs();
	}
	
	public Storage output(int i){
		int t = 0;
		for(Storage output:actorGroup.outputs()){
			if(t == i) return output;
			t++;
		}
		return null;
	}
	
}
