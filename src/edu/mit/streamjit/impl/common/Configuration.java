package edu.mit.streamjit.impl.common;

import com.google.common.base.Function;
import static com.google.common.base.Preconditions.*;
import com.google.common.base.Strings;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.primitives.Ints;
import edu.mit.streamjit.api.Identity;
import edu.mit.streamjit.api.Pipeline;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.blob.BlobFactory;
import edu.mit.streamjit.impl.interp.Interpreter;
import edu.mit.streamjit.util.ReflectionUtils;
import edu.mit.streamjit.util.json.Jsonifier;
import edu.mit.streamjit.util.json.JsonifierFactory;
import edu.mit.streamjit.util.json.Jsonifiers;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

/**
 * A Configuration contains parameters that can be manipulated by the autotuner
 * (or other things).
 *
 * Instances of this class are immutable.  This class uses the builder pattern;
 * to create a Configuration, get a builder by calling Configuration.builder(),
 * add parameters or subconfigurations to it, then call the builder's build()
 * method to build the configuration.
 *
 * Unless otherwise specified, passing null or an empty string to this class'
 * or any parameter class' methods will result in a NullPointerException being
 * thrown.
 * @author Jeffrey Bosboom <jeffreybosboom@gmail.com>
 * @since 3/23/2013
 */
public final class Configuration {
	private final ImmutableMap<String, Parameter> parameters;
	private final ImmutableMap<String, Configuration> subconfigurations;
	private Configuration(ImmutableMap<String, Parameter> parameters, ImmutableMap<String, Configuration> subconfigurations) {
		//We're only called by the builder, so assert, not throw IAE.
		assert parameters != null;
		assert subconfigurations != null;
		this.parameters = parameters;
		this.subconfigurations = subconfigurations;
	}

	/**
	 * Builds Configuration instances.  Parameters and subconfigurations can be
	 * added or removed from this builder; calling build() creates a
	 * Configuration from the current builder state.  Note that build() may be
	 * called more than once; combined with clone(), this allows creating
	 * "prototype" builders that can be cloned, customized, and built.
	 */
	public static final class Builder implements Cloneable {
		private final Map<String, Parameter> parameters;
		private final Map<String, Configuration> subconfigurations;
		/**
		 * Constructs a new Builder.  Called only by Configuration.build().
		 */
		private Builder() {
			//Type inference fail.
			//These maps have their contents copied in the other constructor, so
			//just use these singleton empty maps.
			this(ImmutableMap.<String, Parameter>of(), ImmutableMap.<String, Configuration>of());
		}

		/**
		 * Constructs a new Builder with the given parameters and
		 * subconfigurations.  Called only by Builder.clone().
		 * @param parameters the parameters
		 * @param subconfigurations the subconfigurations
		 */
		private Builder(Map<String, Parameter> parameters, Map<String, Configuration> subconfigurations) {
			//Only called by our own code, so assert.
			assert parameters != null;
			assert subconfigurations != null;
			this.parameters = new HashMap<>(parameters);
			this.subconfigurations = new HashMap<>(subconfigurations);
		}

		public void addParameter(Parameter parameter) {
			checkNotNull(parameter);
			//The parameter constructor should enforce this, so assert.
			assert !Strings.isNullOrEmpty(parameter.getName()) : parameter;
			checkArgument(!parameters.containsKey(parameter.getName()), "conflicting names %s %s", parameters.get(parameter.getName()), parameters);
			parameters.put(parameter.getName(), parameter);
		}

		/**
		 * Removes and returns the parameter with the given name from this
		 * builder, or returns null if this builder doesn't contain a parameter
		 * with that name.
		 * @param name the name of the parameter to remove
		 * @return the removed parameter, or null
		 */
		public Parameter removeParameter(String name) {
			return parameters.remove(checkNotNull(Strings.emptyToNull(name)));
		}

		public void addSubconfiguration(String name, Configuration subconfiguration) {
			checkNotNull(Strings.emptyToNull(name));
			checkNotNull(subconfiguration);
			checkArgument(!subconfigurations.containsKey(name), "name %s already in use", name);
			subconfigurations.put(name, subconfiguration);
		}

		/**
		 * Removes and returns the subconfiguration with the given name from
		 * this builder, or returns null if this builder doesn't contain a
		 * subconfiguration with that name.
		 * @param name the name of the subconfiguration to remove
		 * @return the removed subconfiguration, or null
		 */
		public Configuration removeSubconfiguration(String name) {
			return subconfigurations.remove(checkNotNull(Strings.emptyToNull(name)));
		}

		/**
		 * Builds a new Configuration from the parameters and subconfigurations
		 * added to this builder.  This builder is still valid and may be used
		 * to build more configurations (perhaps after adding or removing
		 * elements), but the returned configurations remain immutable.
		 * @return a new Configuration containing the parameters and
		 * subconfigurations added to this builder
		 */
		public Configuration build() {
			return new Configuration(ImmutableMap.copyOf(parameters), ImmutableMap.copyOf(subconfigurations));
		}

		/**
		 * Returns a copy of this builder.  Subsequent changes to this builder
		 * have no effect on the copy, and vice versa.  This method is useful
		 * for creating "prototype" builders that can be cloned, customized,
		 * and built.
		 * @return a copy of this builder
		 */
		@Override
		public Builder clone() {
			//We're final, so we don't need to use super.clone().
			return new Builder(parameters, subconfigurations);
		}
	}

	/**
	 * Creates a new, empty builder.
	 * @return a new, empty builder
	 */
	public static Builder builder() {
		return new Builder();
	}

	public static Configuration fromJson(String json) {
		return Jsonifiers.fromJson(json, Configuration.class);
	}

	public String toJson() {
		return Jsonifiers.toJson(this).toString();
	}

	/**
	 * JSON-ifies Configurations.  Note that Configuration handles its maps
	 * specially to simplify parsing on the Python side.
	 *
	 * This class is protected with a public constructor to allow ServiceLoader
	 * to instantiate it.
	 */
	protected static final class ConfigurationJsonifier implements Jsonifier<Configuration>, JsonifierFactory {
		public ConfigurationJsonifier() {}
		@Override
		public Configuration fromJson(JsonValue value) {
			JsonObject configObj = Jsonifiers.checkClassEqual(value, Configuration.class);
			JsonObject parametersObj = checkNotNull(configObj.getJsonObject("params"));
			JsonObject subconfigurationsObj = checkNotNull(configObj.getJsonObject("subconfigs"));
			Builder builder = builder();
			for (Map.Entry<String, JsonValue> param : parametersObj.entrySet())
				builder.addParameter(Jsonifiers.fromJson(param.getValue(), Parameter.class));
			for (Map.Entry<String, JsonValue> subconfiguration : subconfigurationsObj.entrySet())
				builder.addSubconfiguration(subconfiguration.getKey(), Jsonifiers.fromJson(subconfiguration.getValue(), Configuration.class));
			return builder.build();
		}

		@Override
		public JsonValue toJson(Configuration t) {
			JsonObjectBuilder paramsBuilder = Json.createObjectBuilder();
			for (Map.Entry<String, Parameter> param : t.parameters.entrySet())
				paramsBuilder.add(param.getKey(), Jsonifiers.toJson(param.getValue()));
			JsonObjectBuilder subconfigsBuilder = Json.createObjectBuilder();
			for (Map.Entry<String, Configuration> subconfig : t.subconfigurations.entrySet())
				subconfigsBuilder.add(subconfig.getKey(), Jsonifiers.toJson(subconfig.getValue()));
			return Json.createObjectBuilder()
					.add("class", Jsonifiers.toJson(Configuration.class))
					.add("params", paramsBuilder)
					.add("subconfigs", subconfigsBuilder)
					//Python-side support
					.add("__module__", "configuration")
					.add("__class__", Configuration.class.getSimpleName())
					.build();
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
			return (Jsonifier<T>)(klass.equals(Configuration.class) ? this : null);
		}
	}

	/**
	 * Returns an immutable mapping of parameter names to the parameters in this
	 * configuration.
	 * @return an immutable mapping of the parameters in this configuration
	 */
	public ImmutableMap<String, Parameter> getParametersMap() {
		return parameters;
	}

	/**
	 * Gets the parameter with the given name, or null if this configuration
	 * doesn't contain a parameter with that name.
	 * @param name the name of the parameter
	 * @return the parameter, or null
	 */
	public Parameter getParameter(String name) {
		return parameters.get(checkNotNull(Strings.emptyToNull(name)));
	}

	/**
	 * Gets the parameter with the given name cast to the given parameter type,
	 * or null if this configuration doesn't contain a parameter with that name.
	 * If this configuration does have a parameter with that name but of a
	 * different type, a ClassCastException will be thrown.
	 * @param <T> the type of the parameter to get
	 * @param name the name of the parameter
	 * @param parameterType the type of the parameter
	 * @return the parameter, or null
	 * @throws ClassCastException if the parameter with the given name exists
	 * but is of a different type
	 */
	public <T extends Parameter> T getParameter(String name, Class<T> parameterType) {
		return checkNotNull(parameterType).cast(getParameter(name));
	}

	/**
	 * Gets the generic parameter with the given name cast to the given
	 * parameter type (including checking the type parameter type), or null if
	 * this configuration doesn't contain a parameter with that name.  If this
	 * configuration does have a parameter with that name but of a different
	 * type or with a different type parameter type,
	 */
	public <U, T extends GenericParameter<?>, V extends GenericParameter<U>> V getParameter(String name, Class<T> parameterType, Class<U> typeParameterType) {
		T parameter = getParameter(name, parameterType);
		//This must be an exact match.
		if (parameter.getGenericParameter() != typeParameterType)
			throw new ClassCastException("Type parameter type mismatch: "+parameter.getGenericParameter() +" != "+typeParameterType);
		//Due to the checks above, this is safe.
		@SuppressWarnings("unchecked")
		V retval = (V)parameter;
		return retval;
	}

	/**
	 * Returns an immutable mapping of subconfiguration names to the
	 * subconfigurations of this configuration.
	 * @return an immutable mapping of the subconfigurations of this
	 * configuration
	 */
	public ImmutableMap<String, Configuration> getSubconfigurationsMap() {
		return subconfigurations;
	}

	/**
	 * Gets the subconfiguration with the given name, or null if this
	 * configuration doesn't contain a subconfiguration with that name.
	 * @param name the name of the subconfiguration
	 * @return the subconfiguration, or null
	 */
	public Configuration getSubconfiguration(String name) {
		return subconfigurations.get(checkNotNull(Strings.emptyToNull(name)));
	}

	/**
	 * A Parameter is a configuration object with a name.  All implementations
	 * of this interface are immutable.
	 *
	 * Users of Configuration shouldn't implement this interface themselves;
	 * instead, use one of the provided implementations in Configuration.
	 */
	public interface Parameter extends Serializable {
		public String getName();
	}

	/**
	 * A GenericParameter is a Parameter with a type parameter. (The name
	 * GenericParameter was chosen in preference to ParameterizedParameter.)
	 *
	 * This interface isn't particularly interesting in and of itself; it mostly
	 * exists to make the Configuration.getParameter(String, Class<T>, Class<U>)
	 * overload have the proper (and checked) return type.
	 * @param <T>
	 */
	public interface GenericParameter<T> extends Parameter {
		public Class<?> getGenericParameter();
	}

	/**
	 * An IntParameter has an integer value that lies within some closed range.
	 * The lower and upper bounds are <b>inclusive</b>.
	 */
	public static final class IntParameter implements Parameter {
		private static final long serialVersionUID = 1L;
		private final String name;
		/**
		 * The Range of this IntParameter.  Note that this range is closed on
		 * both ends.
		 */
		private final Range<Integer> range;
		/**
		 * The value of this IntParameter, which must be contained in the range.
		 */
		private final int value;
		/**
		 * Constructs a new IntParameter.
		 * @param name the parameter's name
		 * @param min the minimum of the range (inclusive)
		 * @param max the maximum of the range (inclusive)
		 * @param value the parameter's value
		 */
		public IntParameter(String name, int min, int max, int value) {
			this(name, Range.closed(min, max), value);
		}
		/**
		 * Constructs a new IntParameter.
		 * @param name the parameter's name
		 * @param range the parameter's range, which must be nonempty and closed
		 * at both ends
		 * @param value the parameter's value
		 */
		public IntParameter(String name, Range<Integer> range, int value) {
			this.name = checkNotNull(Strings.emptyToNull(name));
			checkNotNull(range);
			checkArgument(range.hasLowerBound() && range.lowerBoundType() == BoundType.CLOSED
					&& range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED
					&& !range.isEmpty());
			this.range = range;
			checkArgument(range.contains(value));
			this.value = value;
		}

		protected static final class IntParameterJsonifier implements Jsonifier<IntParameter>, JsonifierFactory {
			public IntParameterJsonifier() {}
			@Override
			public IntParameter fromJson(JsonValue jsonvalue) {
				JsonObject obj = Jsonifiers.checkClassEqual(jsonvalue, IntParameter.class);
				String name = obj.getString("name");
				int min = obj.getInt("min");
				int max = obj.getInt("max");
				int value = obj.getInt("value");
				return new IntParameter(name, min, max, value);
			}

			@Override
			public JsonValue toJson(IntParameter t) {
				return Json.createObjectBuilder()
					.add("class", Jsonifiers.toJson(IntParameter.class))
					.add("name", t.getName())
					.add("min", t.getMin())
					.add("max", t.getMax())
					.add("value", t.getValue())
					//Python-side support
					.add("__module__", "parameters")
					.add("__class__", IntParameter.class.getSimpleName())
					.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(IntParameter.class) ? this : null);
			}
		}
		@Override
		public String getName() {
			return name;
		}
		public int getMin() {
			return range.lowerEndpoint();
		}
		public int getMax() {
			return range.upperEndpoint();
		}
		public Range<Integer> getRange() {
			return range;
		}
		public int getValue() {
			return value;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final IntParameter other = (IntParameter)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Objects.equals(this.range, other.range))
				return false;
			if (this.value != other.value)
				return false;
			return true;
		}
		@Override
		public int hashCode() {
			int hash = 3;
			hash = 97 * hash + Objects.hashCode(this.name);
			hash = 97 * hash + Objects.hashCode(this.range);
			hash = 97 * hash + this.value;
			return hash;
		}
		@Override
		public String toString() {
			return String.format("[%s: %d in %s]", name, value, range);
		}
	}

	/**
	 * A SwitchParameter represents a choice of one of some universe of objects.
	 * For example, a SwitchParameter<Boolean> is a simple on-off flag, while a
	 * SwitchParameter<ChannelFactory> represents a choice of factories.
	 *
	 * The autotuner assumes there's no numeric relationship between the objects
	 * in the universe, in contrast to IntParameter, for which it will try to
	 * fit a model.
	 *
	 * The order of a SwitchParameter's universe is relevant for equals() and
	 * hashCode() and correct operation of the autotuner.  (To the autotuner, a
	 * SwitchParameter is just an integer between 0 and the universe size; if
	 * the order of the universe changes, the meaning of that integer changes
	 * and the autotuner will get confused.)
	 *
	 * Objects put into SwitchParameters must implements equals() and hashCode()
	 * for SwitchParameter's equals() and hashCode() methods to work correctly.
	 * Objects put into SwitchParameters must be immutable.
	 *
	 * To the extent possible, the type T should not itself contain type
	 * parameters.  Consider defining a new class or interface that fixes the
	 * type parameters.
	 *
	 * TODO: restrictions required for JSON representation: toString() and
	 * fromString/valueOf/String ctor, fallback to base64 encoded Serializable,
	 * etc; List/Set etc. not good unless contains only one type (e.g.,
	 * List<String> can be handled okay)
	 * @param <T>
	 */
	public static final class SwitchParameter<T> implements GenericParameter<Boolean> {
		private static final long serialVersionUID = 1L;
		private final String name;
		/**
		 * The type of elements in this SwitchParameter.
		 */
		private final Class<T> type;
		/**
		 * The universe of this SwitchParameter -- must not contain any
		 * duplicate elements.
		 */
		private final ImmutableList<T> universe;
		/**
		 * The index of the value in the universe.  Note that most of the
		 * interface prefers to work with Ts rather than values.
		 */
		private final int value;

		/**
		 * Create a new SwitchParameter with the given type, value, and
		 * universe.  The universe must contain at least one element, contain no
		 * duplicate elements, and contain the value.
		 *
		 * The type must be provided explicitly, rather than being inferred as
		 * value.getClass(), as value might be of a more-derived type than the
		 * elements in the universe.
		 * @param name the name of this parameter
		 * @param type the type of the universe
		 * @param value the value of this parameter
		 * @param universe the universe of possible values of this parameter
		 */
		public SwitchParameter(String name, Class<T> type, T value, Iterable<? extends T> universe) {
			this.name = checkNotNull(Strings.emptyToNull(name));
			this.type = checkNotNull(type);
			int size = 0;
			ImmutableSet.Builder<T> builder = ImmutableSet.builder();
			for (T t : universe) {
				checkArgument(!ReflectionUtils.usesObjectEquality(t.getClass()), "all objects in universe must have proper equals()/hashCode()");
				builder.add(t);
				++size;
			}
			ImmutableSet<T> set = builder.build();
			checkArgument(set.size() == size, "universe contains duplicate elements");
			//A single element universe is permitted, through not particularly
			//useful.
			checkArgument(set.size() > 0, "empty universe");
			this.universe = set.asList();
			this.value = checkElementIndex(this.universe.indexOf(value), this.universe.size(), "value not in universe");
		}

		/**
		 * Creates a new SwitchParameter<Boolean> with the given name and value.
		 * The universe is [false, true].
		 * @param name the name of this parameter
		 * @param value the value of this parameter (true or false)
		 * @return a new SwitchParameter<Boolean> with the given name and value
		 */
		public static SwitchParameter<Boolean> create(String name, boolean value) {
			return new SwitchParameter<>(name, Boolean.class, value, Arrays.asList(false, true));
		}

		protected static final class SwitchParameterJsonifier implements Jsonifier<SwitchParameter<?>>, JsonifierFactory {
			public SwitchParameterJsonifier() {}
			@Override
			@SuppressWarnings({"unchecked", "rawtypes"})
			public SwitchParameter<?> fromJson(JsonValue jsonvalue) {
				JsonObject obj = Jsonifiers.checkClassEqual(jsonvalue, SwitchParameter.class);
				String name = obj.getString("name");
				Class<?> universeType = Jsonifiers.fromJson(obj.get("universeType"), Class.class);
				ImmutableList<?> universe = ImmutableList.copyOf(Jsonifiers.fromJson(obj.get("universe"), ReflectionUtils.getArrayType(universeType)));
				//We should have caught this in fromJson(v, universeType).
				assert Jsonifiers.notHeapPolluted(universe, universeType);
				int value = obj.getInt("value");
				return new SwitchParameter(name, universeType, universe.get(value), universe);
			}

			@Override
			public JsonValue toJson(SwitchParameter<?> t) {
				return Json.createObjectBuilder()
						.add("class", Jsonifiers.toJson(SwitchParameter.class))
						.add("name", t.getName())
						.add("universeType", Jsonifiers.toJson(t.type))
						.add("universe", Jsonifiers.toJson(t.universe.toArray()))
						.add("value", t.value)
						//Python-side support
						.add("__module__", "parameters")
						.add("__class__", SwitchParameter.class.getSimpleName())
						.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(SwitchParameter.class) ? this : null);
			}
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public Class<T> getGenericParameter() {
			return type;
		}

		/**
		 * Gets this parameter's value.
		 * @return this parameter's value
		 */
		public T getValue() {
			return universe.get(value);
		}

		/**
		 * Gets the universe of possible values for this parameter.
		 * @return the universe of possible values for this parameter
		 */
		public ImmutableList<T> getUniverse() {
			return universe;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final SwitchParameter<?> other = (SwitchParameter<?>)obj;
			if (!Objects.equals(this.type, other.type))
				return false;
			if (!Objects.equals(this.universe, other.universe))
				return false;
			if (this.value != other.value)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 7;
			hash = 47 * hash + Objects.hashCode(this.type);
			hash = 47 * hash + Objects.hashCode(this.universe);
			hash = 47 * hash + this.value;
			return hash;
		}

		@Override
		public String toString() {
			return String.format("[%s: %s (index %d) of %s]", name, getValue(), value, universe);
		}
	}

	/**
	 * A PartitionParameter represents a partitioning of a stream graph
	 * (workers) into Blobs, the kind of those Blobs, and the mapping of Blobs
	 * to cores on machines.
	 * <p/>
	 * For the purposes of this class, machines are considered distinct, but
	 * cores on the same machine are not.
	 */
	public static final class PartitionParameter implements Parameter {
		private static final long serialVersionUID = 1L;
		private final String name;
		/**
		 * The number of cores on each machine. Always contains at least one
		 * element and all elements are always >= 1.
		 */
		private final ImmutableList<Integer> coresPerMachine;
		/**
		 * A list per machine of a list of blobs on that machine. The inner
		 * lists are sorted.
		 */
		private final ImmutableList<ImmutableList<BlobSpecifier>> blobs;
		/**
		 * The BlobFactories that can be used to create blobs. This list
		 * contains no duplicate elements.
		 */
		private final ImmutableList<BlobFactory> blobFactoryUniverse;
		/**
		 * The maximum identifier of a worker in the stream graph, used during
		 * deserialization to check that all workers have been assigned to a
		 * blob.
		 */
		private final int maxWorkerIdentifier;

		/**
		 * Only called by the builder.
		 */
		private PartitionParameter(String name, ImmutableList<Integer> coresPerMachine, ImmutableList<ImmutableList<BlobSpecifier>> blobs, ImmutableList<BlobFactory> blobFactoryUniverse, int maxWorkerIdentifier) {
			this.name = name;
			this.coresPerMachine = coresPerMachine;
			this.blobs = blobs;
			this.blobFactoryUniverse = blobFactoryUniverse;
			this.maxWorkerIdentifier = maxWorkerIdentifier;
		}

		public static final class Builder {
			private final String name;
			private final ImmutableList<Integer> coresPerMachine;
			private final int[] coresAvailable;
			private final List<BlobFactory> blobFactoryUniverse = new ArrayList<>();
			private final List<List<BlobSpecifier>> blobs = new ArrayList<>();
			private final NavigableSet<Integer> workersInBlobs = new TreeSet<>();

			private Builder(String name, ImmutableList<Integer> coresPerMachine) {
				this.name = name;
				this.coresPerMachine = coresPerMachine;
				this.coresAvailable = Ints.toArray(this.coresPerMachine);
				//You might think we can use Collections.nCopies() here, but
				//that would mean all cores would share the same list!
				for (int i = 0; i < coresPerMachine.size(); ++i)
					blobs.add(new ArrayList<BlobSpecifier>());
			}

			public void addBlobFactory(BlobFactory factory) {
				checkArgument(!ReflectionUtils.usesObjectEquality(factory.getClass()), "blob factories must have a proper equals() and hashCode()");
				checkArgument(!blobFactoryUniverse.contains(checkNotNull(factory)), "blob factory already added");
				blobFactoryUniverse.add(factory);
			}

			public void addBlob(int machine, int cores, BlobFactory blobFactory, Set<Worker<?, ?>> workers) {
				checkElementIndex(machine, coresPerMachine.size());
				checkArgument(cores <= coresAvailable[machine],
						"allocating %s cores but only %s available on machine %s",
						cores, coresAvailable[machine], machine);
				checkArgument(blobFactoryUniverse.contains(blobFactory),
						"blob factory %s not in universe %s", blobFactory, blobFactoryUniverse);
				ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
				for (Worker<?, ?> worker : workers) {
					int identifier = Workers.getIdentifier(worker);
					checkArgument(identifier >= 0, "uninitialized worker identifier: %s", worker);
					checkArgument(!workersInBlobs.contains(identifier), "worker %s already assigned to blob", worker);
					builder.add(identifier);
				}
				ImmutableSortedSet<Integer> workerIdentifiers = builder.build();

				//Okay, we've checked everything.  Commit.
				blobs.get(machine).add(new BlobSpecifier(workerIdentifiers, machine, cores, blobFactory));
				workersInBlobs.addAll(workerIdentifiers);
				coresAvailable[machine] -= cores;
			}

			public PartitionParameter build() {
				ImmutableList.Builder<ImmutableList<BlobSpecifier>> blobBuilder = ImmutableList.builder();
				for (List<BlobSpecifier> list : blobs) {
					Collections.sort(list);
					blobBuilder.add(ImmutableList.copyOf(list));
				}
				return new PartitionParameter(name, coresPerMachine, blobBuilder.build(), ImmutableList.copyOf(blobFactoryUniverse), workersInBlobs.last());
			}
		}

		public static Builder builder(String name, List<Integer> coresPerMachine) {
			checkArgument(!coresPerMachine.isEmpty());
			for (Integer i : coresPerMachine)
				checkArgument(checkNotNull(i) >= 1);
			return new Builder(checkNotNull(Strings.emptyToNull(name)), ImmutableList.copyOf(coresPerMachine));
		}

		public static Builder builder(String name, int... coresPerMachine) {
			return builder(name, Ints.asList(coresPerMachine));
		}

		/**
		 * A blob's properties.
		 */
		public static final class BlobSpecifier implements Comparable<BlobSpecifier> {
			/**
			 * The identifiers of the workers in this blob.
			 */
			private final ImmutableSortedSet<Integer> workerIdentifiers;
			/**
			 * The index of the machine this blob is on.
			 */
			private final int machine;
			/**
			 * The number of cores allocated to this blob.
			 */
			private final int cores;
			/**
			 * The BlobFactory to be used to create this blob.
			 */
			private final BlobFactory blobFactory;

			private BlobSpecifier(ImmutableSortedSet<Integer> workerIdentifiers, int machine, int cores, BlobFactory blobFactory) {
				this.workerIdentifiers = workerIdentifiers;
				checkArgument(machine >= 0);
				this.machine = machine;
				checkArgument(cores >= 1, "all blobs must be assigned at least one core");
				this.cores = cores;
				this.blobFactory = blobFactory;
			}

			protected static final class BlobSpecifierJsonifier implements Jsonifier<BlobSpecifier>, JsonifierFactory {
				public BlobSpecifierJsonifier() {}
				@Override
				public BlobSpecifier fromJson(JsonValue value) {
					//TODO: array serialization, error checking
					JsonObject obj = Jsonifiers.checkClassEqual(value, BlobSpecifier.class);
					int machine = obj.getInt("machine");
					int cores = obj.getInt("cores");
					BlobFactory blobFactory = Jsonifiers.fromJson(obj.get("blobFactory"), BlobFactory.class);
					ImmutableSortedSet.Builder<Integer> builder = ImmutableSortedSet.naturalOrder();
					for (JsonValue i : obj.getJsonArray("workerIds"))
						builder.add(Jsonifiers.fromJson(i, Integer.class));
					return new BlobSpecifier(builder.build(), machine, cores, blobFactory);
				}
				@Override
				public JsonValue toJson(BlobSpecifier t) {
					JsonArrayBuilder workerIds = Json.createArrayBuilder();
					for (int i : t.workerIdentifiers)
						workerIds.add(i);
					return Json.createObjectBuilder()
							.add("class", Jsonifiers.toJson(BlobSpecifier.class))
							.add("machine", t.machine)
							.add("cores", t.cores)
							.add("blobFactory", Jsonifiers.toJson(t.blobFactory))
							.add("workerIds", workerIds)
							//Python-side support
							.add("__module__", "configuration")
							.add("__class__", BlobSpecifier.class.getSimpleName())
							.build();
				}
				@Override
				@SuppressWarnings("unchecked")
				public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
					return (Jsonifier<T>)(klass.equals(BlobSpecifier.class) ? this : null);
				}
			}

			public ImmutableSortedSet<Integer> getWorkerIdentifiers() {
				return workerIdentifiers;
			}

			public ImmutableSet<Worker<?, ?>> getWorkers(Worker<?, ?> streamGraph) {
				ImmutableSet<Worker<?, ?>> allWorkers = Workers.getAllWorkersInGraph(streamGraph);
				ImmutableMap<Integer, Worker<?, ?>> workersByIdentifier =
						Maps.uniqueIndex(allWorkers, new Function<Worker<?, ?>, Integer>() {
					@Override
					public Integer apply(Worker<?, ?> input) {
						return Workers.getIdentifier(input);
					}
				});
				ImmutableSet.Builder<Worker<?, ?>> workersInBlob = ImmutableSet.builder();
				for (Integer i : workerIdentifiers) {
					Worker<?, ?> w = workersByIdentifier.get(i);
					if (w == null)
						throw new IllegalArgumentException("Identifier " + i + " not in given stream graph");
					workersInBlob.add(w);
				}
				return workersInBlob.build();
			}

			public int getMachine() {
				return machine;
			}

			public int getCores() {
				return cores;
			}

			public BlobFactory getBlobFactory() {
				return blobFactory;
			}

			@Override
			public int hashCode() {
				int hash = 3;
				hash = 37 * hash + Objects.hashCode(this.workerIdentifiers);
				hash = 37 * hash + this.machine;
				hash = 37 * hash + this.cores;
				hash = 37 * hash + Objects.hashCode(this.blobFactory);
				return hash;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null)
					return false;
				if (getClass() != obj.getClass())
					return false;
				final BlobSpecifier other = (BlobSpecifier)obj;
				if (!Objects.equals(this.workerIdentifiers, other.workerIdentifiers))
					return false;
				if (this.machine != other.machine)
					return false;
				if (this.cores != other.cores)
					return false;
				if (!Objects.equals(this.blobFactory, other.blobFactory))
					return false;
				return true;
			}

			@Override
			public int compareTo(BlobSpecifier o) {
				//Worker identifiers are unique within the stream graph, so
				//we can base our comparison on them.
				return workerIdentifiers.first().compareTo(o.workerIdentifiers.first());
			}
		}

		protected static final class PartitionParameterJsonifier implements Jsonifier<PartitionParameter>, JsonifierFactory {
			public PartitionParameterJsonifier() {}
			@Override
			public PartitionParameter fromJson(JsonValue value) {
				//TODO: array serialization, error checking
				JsonObject obj = Jsonifiers.checkClassEqual(value, PartitionParameter.class);
				String name = obj.getString("name");
				int maxWorkerIdentifier = obj.getInt("maxWorkerIdentifier");
				ImmutableList.Builder<Integer> coresPerMachine = ImmutableList.builder();
				for (JsonValue v : obj.getJsonArray("coresPerMachine"))
					coresPerMachine.add(Jsonifiers.fromJson(v, Integer.class));
				ImmutableList.Builder<BlobFactory> blobFactoryUniverse = ImmutableList.builder();
				for (JsonValue v : obj.getJsonArray("blobFactoryUniverse"))
					blobFactoryUniverse.add(Jsonifiers.fromJson(v, BlobFactory.class));
				List<List<BlobSpecifier>> mBlobs = new ArrayList<>();
				for (int i = 0; i < coresPerMachine.build().size(); ++i)
					mBlobs.add(new ArrayList<BlobSpecifier>());
				for (JsonValue v : obj.getJsonArray("blobs")) {
					BlobSpecifier bs = Jsonifiers.fromJson(v, BlobSpecifier.class);
					mBlobs.get(bs.getMachine()).add(bs);
				}
				ImmutableList.Builder<ImmutableList<BlobSpecifier>> blobs = ImmutableList.builder();
				for (List<BlobSpecifier> m : mBlobs)
					blobs.add(ImmutableList.copyOf(m));
				return new PartitionParameter(name, coresPerMachine.build(), blobs.build(), blobFactoryUniverse.build(), maxWorkerIdentifier);
			}

			@Override
			public JsonValue toJson(PartitionParameter t) {
				JsonArrayBuilder coresPerMachine = Json.createArrayBuilder();
				for (int i : t.coresPerMachine)
					coresPerMachine.add(i);
				JsonArrayBuilder blobFactoryUniverse = Json.createArrayBuilder();
				for (BlobFactory factory : t.blobFactoryUniverse)
					blobFactoryUniverse.add(Jsonifiers.toJson(factory));
				JsonArrayBuilder blobs = Json.createArrayBuilder();
				for (List<BlobSpecifier> machine : t.blobs)
					for (BlobSpecifier blob : machine)
						blobs.add(Jsonifiers.toJson(blob));
				return Json.createObjectBuilder()
						.add("class", Jsonifiers.toJson(PartitionParameter.class))
						.add("name", t.getName())
						.add("maxWorkerIdentifier", t.maxWorkerIdentifier)
						.add("coresPerMachine", coresPerMachine)
						.add("blobFactoryUniverse", blobFactoryUniverse)
						.add("blobs", blobs)
						//Python-side support
						.add("__module__", "parameters")
						.add("__class__", PartitionParameter.class.getSimpleName())
						.build();
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> Jsonifier<T> getJsonifier(Class<T> klass) {
				return (Jsonifier<T>)(klass.equals(PartitionParameter.class) ? this : null);
			}
		}

		@Override
		public String getName() {
			return name;
		}

		public int getMachineCount() {
			return coresPerMachine.size();
		}

		public int getCoresOnMachine(int machine) {
			return coresPerMachine.get(machine);
		}

		public ImmutableList<BlobSpecifier> getBlobsOnMachine(int machine) {
			return blobs.get(machine);
		}

		public ImmutableList<BlobFactory> getBlobFactories() {
			return blobFactoryUniverse;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final PartitionParameter other = (PartitionParameter)obj;
			if (!Objects.equals(this.name, other.name))
				return false;
			if (!Objects.equals(this.coresPerMachine, other.coresPerMachine))
				return false;
			if (!Objects.equals(this.blobs, other.blobs))
				return false;
			if (!Objects.equals(this.blobFactoryUniverse, other.blobFactoryUniverse))
				return false;
			if (this.maxWorkerIdentifier != other.maxWorkerIdentifier)
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int hash = 3;
			hash = 61 * hash + Objects.hashCode(this.name);
			hash = 61 * hash + Objects.hashCode(this.coresPerMachine);
			hash = 61 * hash + Objects.hashCode(this.blobs);
			hash = 61 * hash + Objects.hashCode(this.blobFactoryUniverse);
			hash = 61 * hash + this.maxWorkerIdentifier;
			return hash;
		}
	}

	public static void main(String[] args) {
		Configuration.Builder builder = Configuration.builder();
		builder.addParameter(new IntParameter("foo", 0, 10, 8));
		builder.addParameter(SwitchParameter.create("bar", true));

		Identity<Integer> first = new Identity<>(), second = new Identity<>();
		Pipeline<Integer, Integer> pipeline = new Pipeline<>(first, second);
		ConnectWorkersVisitor cwv = new ConnectWorkersVisitor();
		pipeline.visit(cwv);

		PartitionParameter.Builder partParam = PartitionParameter.builder("part", 1, 1);
		BlobFactory factory = new Interpreter.InterpreterBlobFactory();
		partParam.addBlobFactory(factory);
		partParam.addBlob(0, 1, factory, Collections.<Worker<?, ?>>singleton(first));
		partParam.addBlob(1, 1, factory, Collections.<Worker<?, ?>>singleton(second));
		builder.addParameter(partParam.build());

		Configuration cfg1 = builder.build();
		String json = Jsonifiers.toJson(cfg1).toString();
		System.out.println(json);
		Configuration cfg2 = Jsonifiers.fromJson(json, Configuration.class);
		System.out.println(cfg2);
		String json2 = Jsonifiers.toJson(cfg2).toString();
		System.out.println(json2);

		/*Configuration.Builder builder = Configuration.builder();
		builder.addParameter(new IntParameter("foo", 0, 10, 8));
		Configuration cfg1 = builder.build();
		String json = Jsonifiers.toJson(cfg1).toString();
		System.out.println(json);*/

	}
}
