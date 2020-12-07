package cp1.solution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import cp1.base.Resource;
import cp1.base.ResourceOperation;

public class Transaction implements Comparable<Transaction> {
	private final long startDate;
	private final Thread myThread;
	
	@Override
	public int compareTo(Transaction tr) {
		long diff;
		
		if (startDate == tr.startDate) {
			diff = myThread.getId() - tr.myThread.getId();
		}
		else {
			diff = startDate - tr.startDate;
		}
		
		if (diff > 0) {
			return 1;
		}
		else if (diff == 0){
			return 0;
		}
		else {
			return -1;
		}
	}
	
	@Override
	public boolean equals(Object tr) {
		if (tr.getClass() != Transaction.class) {
			return false;
		}
		else {
			return compareTo((Transaction)tr) == 0;
		}
	}
	
	private boolean isAborted = false;
	
	public void abort() {
		isAborted = true;
		myThread.interrupt();
	}
	
	public boolean isFlgAborted() {
		return isAborted;
	}
	
	private boolean isLocked = false;
	
	public void lock() {
		isLocked = true;
	}
	
	public void unlock() {
		isLocked = false;
	}
	
	public boolean isFlgLocked() {
		return isLocked;
	}
	
	private final Set<Transaction> whoWaitsOnMe = new HashSet<Transaction>();
	
	public void addWaiting(Transaction tr) {
		whoWaitsOnMe.add(tr);
	}
	
	public void resolveDeadlock() {
		for (Transaction tr: whoWaitsOnMe) {
			tr.waitingFor = null;
		}
		
		if (waitingFor != null) {
			waitingFor.whoWaitsOnMe.remove(this);
		}
		
		abort();
	}
	
	protected Transaction(long startDate) {
		this.startDate = startDate;
		this.myThread = Thread.currentThread();
	}
	
	// dummy
	protected Transaction() {
		startDate = 0;
		myThread = null;
	}
	
	// Long for how many times the resource has been operated on.
	private final Map<Resource, Long> resourcesInUse = new HashMap<Resource, Long>();
	
	public void addResource(Resource r) {
		resourcesInUse.put(r, (long)0);
	}
	
	public boolean isUsingResource(Resource r) {
		return resourcesInUse.containsKey(r);
	}
	
	public Iterator<Resource> getResourcesIterator() {
		return resourcesInUse.keySet().iterator();
	}
	
	private final Stack<Pair<ResourceOperation, Resource>> stackTrace = new Stack<>();
	
	public boolean isStackTraceEmpty() {
		return stackTrace.empty();
	}
	
	public Pair<ResourceOperation, Resource> stackTracePop() {
		return stackTrace.pop();
	}
	
	public void registerOperation(ResourceOperation ro, Resource r) {
		stackTrace.push(new Pair<ResourceOperation, Resource>(ro, r));
		long rcount = resourcesInUse.get(r);
		resourcesInUse.replace(r, rcount + 1);
	}
	
	// true - the last action using this Resource has been rollbacked
	public boolean rollbackResource(Resource r) {
		long rcount = resourcesInUse.get(r);
		assert rcount != 0;
		
		if (rcount == 1) {
			resourcesInUse.remove(r);
			return true;
		}
		else {
			resourcesInUse.replace(r, --rcount);
			return false;
		}
	}
	
	private Transaction waitingFor = null;
	
	public void setWaitFor(Transaction waiting) {
		waitingFor = waiting;
		
		if (waiting != null) {
			waiting.addWaiting(this);
		}
	}
	
	public Transaction getNext() {
		return waitingFor;
	}
	
}
