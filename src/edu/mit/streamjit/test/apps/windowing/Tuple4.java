package edu.mit.streamjit.test.apps.windowing;

public class Tuple4<T0, T1, T2, T3> {

	private static final long serialVersionUID = 1L;

	/** Field 0 of the tuple. */
	public T0 f0;
	/** Field 1 of the tuple. */
	public T1 f1;
	/** Field 2 of the tuple. */
	public T2 f2;
	/** Field 3 of the tuple. */
	public T3 f3;

	/**
	 * Creates a new tuple where all fields are null.
	 */
	public Tuple4() {}

	/**
	 * Creates a new tuple and assigns the given values to the tuple's fields.
	 *
	 * @param value0 The value for field 0
	 * @param value1 The value for field 1
	 * @param value2 The value for field 2
	 * @param value3 The value for field 3
	 */
	public Tuple4(T0 value0, T1 value1, T2 value2, T3 value3) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
	}

	public void setFields(T0 value0, T1 value1, T2 value2, T3 value3) {
		this.f0 = value0;
		this.f1 = value1;
		this.f2 = value2;
		this.f3 = value3;
	}


	public String toString() {
		return "(" + String.valueOf(this.f0)
			+ "," + String.valueOf(this.f1)
			+ "," + String.valueOf(this.f2)
			+ "," + String.valueOf(this.f3)
			+ ")";
	}
	
	public String toStringC() {
		return "Check(" + String.valueOf(this.f0)
			+ "," + String.valueOf(this.f1)
			+ "," + String.valueOf(this.f2)
			+ "," + String.valueOf(this.f3)
			+ ")";
	}

	public boolean equals(Object o) {
		if(this == o) { return true; }
		if (!(o instanceof Tuple4)) { return false; }
		@SuppressWarnings("rawtypes")
		Tuple4 tuple = (Tuple4) o;
		if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) { return false; }
		if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) { return false; }
		if (f2 != null ? !f2.equals(tuple.f2) : tuple.f2 != null) { return false; }
		if (f3 != null ? !f3.equals(tuple.f3) : tuple.f3 != null) { return false; }
		return true;
	}


	public int hashCode() {
		int result = f0 != null ? f0.hashCode() : 0;
		result = 31 * result + (f1 != null ? f1.hashCode() : 0);
		result = 31 * result + (f2 != null ? f2.hashCode() : 0);
		result = 31 * result + (f3 != null ? f3.hashCode() : 0);
		return result;
	}

	public static <T0,T1,T2,T3> Tuple4<T0,T1,T2,T3> of(T0 value0, T1 value1, T2 value2, T3 value3) {
		return new Tuple4<T0,T1,T2,T3>(value0, value1, value2, value3);
	}

	public void add(Tuple4<T0, T1, T2, T3> current) {
		this.f0 = current.f0;
		this.f1 = current.f1;
		this.f2 = current.f2;
		this.f3 = current.f3;
		
	}

}