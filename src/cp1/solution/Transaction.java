package cp1.solution;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import cp1.base.Resource;
import cp1.base.ResourceOperation;

public class Transaction {
	private final long startDate;
	private final Thread myThread;
	
	private boolean isAborted = false;
	
	public void abort() {
		isAborted = true;
		myThread.interrupt();
	}
	
	private final Set<Transaction> whoWaitsOnMe = new Set<Transaction>();
	
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
	
	private Transaction(long startDate) {
		this.startDate = startDate;
		this.myThread = Thread.currentThread();
	}
	
	// Long for how many times the resource has been operated on.
	private final Map<Resource, Long> resourcesInUse = new HashMap<Resource, Long>();
	private final Stack<Pair<ResourceOperation, Resource>> stackTrace = new Stack<>();
	
	private Transaction waitingFor = null;
	
	public void setWaitFor(Transaction waiting) {
		waitingFor = waiting;
		waiting.addWaiting(this);
	}
	
	public Transaction getNext() {
		return waitingFor;
	}
	
	public boolean isNewerThan(Transaction tr) {
		return
			this.startingDate > tr.startingDate || (
				this.startingDate == tr.startingDate &&
				this.myThread.getId() > tr.myThread.getId()
			)
	}
	
}
