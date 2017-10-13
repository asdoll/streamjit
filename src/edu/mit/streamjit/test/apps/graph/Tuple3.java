package edu.mit.streamjit.test.apps.graph;


/**
 * A tuple with 3 fields. Tuples are strongly typed; each field may be of a separate type.
 * The fields of the tuple can be accessed directly as public fields (f0, f1, ...) or via their position
 * through the {@link #getField(int)} method. The tuple field positions start at zero.
 * <p>
 * Tuples are mutable types, meaning that their fields can be re-assigned. This allows functions that work
 * with Tuples to reuse objects in order to reduce pressure on the garbage collector.
 *
 * @see Tuple
 *
 * @param <T0> The type of field 0
 * @param <T1> The type of field 1
 * @param <T2> The type of field 2
 */
public class Tuple3<T0, T1, T2> {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f0;
	/** Field 1 of the tuple. */
	public T1 f1;
	/** Field 2 of the tuple. */
	public T2 f2;

	/**
	 * Creates a new tuple where all fields are null.
	 */
	public Tuple3() {}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 */
	public Tuple3(T0 value0, T1 value1, T2 value2) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
	}

	public int getArity() { return 3; }

	
	public <T> T getField(int pos) {
		switch(pos) {
			case 0: return (T) this.f0;
			case 1: return (T) this.f1;
			case 2: return (T) this.f2;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}

	public <T> void setField(T value, int pos) {
		switch(pos) {
			case 0:
				this.f0 = (T0) value;
				break;
			case 1:
				this.f1 = (T1) value;
				break;
			case 2:
				this.f2 = (T2) value;
				break;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}


	public void setFields(T0 value0, T1 value1, T2 value2) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
	}



	public String toString() {
		return "(" + String.valueOf(this.f0)
			+ "," + String.valueOf(this.f1)
			+ "," + String.valueOf(this.f2)
			+ ")";
	}


	public boolean equals(Object o) {
		if(this == o) { return true; }
		if (!(o instanceof Tuple3)) { return false; }
		@SuppressWarnings("rawtypes")
		Tuple3 tuple = (Tuple3) o;
		if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) { return false; }
		if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) { return false; }
		if (f2 != null ? !f2.equals(tuple.f2) : tuple.f2 != null) { return false; }
		return true;
	}


	public int hashCode() {
		int result = f0 != null ? f0.hashCode() : 0;
		result = 31 * result + (f1 != null ? f1.hashCode() : 0);
		result = 31 * result + (f2 != null ? f2.hashCode() : 0);
		return result;
	}

	public Tuple3<T0,T1,T2> copy(){ 
		return new Tuple3<T0,T1,T2>(this.f0,
			this.f1,
			this.f2);
	}

	public static <T0,T1,T2> Tuple3<T0,T1,T2> of(T0 value0, T1 value1, T2 value2) {
		return new Tuple3<T0,T1,T2>(value0, value1, value2);
	}
}
