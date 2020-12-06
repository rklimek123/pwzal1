package cp1.solution;

public class Pair<T1, T2> {
	
	private final T1 first;
	private final T2 second;
	
	public T1 first() {
		return first;
	}
	
	public T2 second() {
		return second;
	}
	
	public Pair(
			T1 first,
			T2 second
	) {
		this.first = first;
		this.second = second;
	}
	
	
}
