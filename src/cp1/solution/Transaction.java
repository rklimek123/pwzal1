package cp1.solution;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import cp1.base.Resource;
import cp1.base.ResourceOperation;

public class Transaction {
	private final long startDate;
	
	public long getStartingDate() {
		return this.startDate;
	}
	
	private Transaction(long startDate) {
		this.startDate = startDate;
	}
	
	private boolean isAborted = false;
	
	public void abort() {
		isAborted = true;
	}
	
	private final Set<Resource> resourcesInUse = new HashSet<Resource>();
	private final Stack<Pair<ResourceOperation, Resource>> stackTrace = new Stack<>();
	
	// find and union when resolving possible deadlocks
	// disjoin additionally
	// Z find and union sa problemy odnosnie wspolbieznosci
	// lockami tez tego nie zalatwie, lock klasowy blokuej wszystkie grupy
	// nie ma locka na grupe tylko, co daloby sie robic, gdyby przechowywac wierzcholki w jakims secie
	private Set<Transaction> whoWaits = new HashSet<Transaction>();
	
	private Transaction waitingDirect = this;
	private Transaction groupId = this;
	
}
