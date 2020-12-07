/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Rafal Klimek (rk418291@students.mimuw.edu.pl)
 */
package cp1.solution;

import java.util.Collection;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import cp1.base.LocalTimeProvider;
import cp1.base.Resource;
import cp1.base.ResourceId;
import cp1.base.ResourceOperation;
import cp1.base.ResourceOperationException;
import cp1.base.ActiveTransactionAborted;
import cp1.base.NoActiveTransactionException;
import cp1.base.AnotherTransactionActiveException;
import cp1.base.TransactionManager;
import cp1.base.UnknownResourceIdException;

/**
 * The transaction manager implementation
 * 
 * @author Rafal Klimek (rk418291@students.mimuw.edu.pl)
 */
public class MT implements TransactionManager {
	
	private final Collection<Resource> resources;
	private final LocalTimeProvider timeProvider;
	
	private static Transaction dummyTransaction = new Transaction();

	private final ConcurrentMap<Thread, Transaction> activeTransactions;
	private final ConcurrentMap<Resource, Transaction> whoHasAccess;
	private final ConcurrentMap<Resource, Queue<Transaction>> whoWaitsOnResource;
	
	// Taken when modifying the structure of a waiting graph
	private final Object deadlockResolveLock = new Object();
	
	protected MT(
			Collection<Resource> resources,
			LocalTimeProvider timeProvider
	) {
		this.resources = resources;
		this.timeProvider = timeProvider;
		
		activeTransactions = new ConcurrentHashMap<Thread, Transaction>();
		whoHasAccess = new ConcurrentHashMap<Resource, Transaction>();
		whoWaitsOnResource = new ConcurrentHashMap<Resource, Queue<Transaction>>();
		
		for (Resource r: resources) {
			whoHasAccess.putIfAbsent(r, dummyTransaction);
			whoWaitsOnResource.putIfAbsent(r, new PriorityQueue<Transaction>());
		}
	}
	
	@Override
	public void startTransaction(
	) throws
		AnotherTransactionActiveException {
		
		if (isTransactionActive())
			throw new AnotherTransactionActiveException();
		
		Thread current = Thread.currentThread();
		Transaction tr = new Transaction(timeProvider.getTime());
		
		if (isTransactionActive())
			activeTransactions.replace(current, tr);
		else
			activeTransactions.putIfAbsent(current, tr);
	}
	
	
	
	@Override
	public void operateOnResourceInCurrentTransaction(
			ResourceId rid,
			ResourceOperation operation
	) throws
		NoActiveTransactionException,
		UnknownResourceIdException,
		ActiveTransactionAborted,
		ResourceOperationException,
		InterruptedException {
		
		if (!isTransactionActive())
			throw new NoActiveTransactionException();
		
		Resource resource = null;
		
		for (Resource r: resources) {
			if (r.getId() == rid) {
				resource = r;
				break;
			}
		}
		
		if (resource == null)
			throw new UnknownResourceIdException(rid);
		
		if (isTransactionAborted()) {
			throw new ActiveTransactionAborted();
		}
		
		Transaction currentTr = tryGetActiveTransaction();
		assert currentTr != null;
		
		if (!currentTr.isUsingResource(resource)) {
			synchronized (resource) {
				while (whoHasAccess.getOrDefault(resource, dummyTransaction) != dummyTransaction ||
						currentTr.isFlgLocked()) {
					
					Transaction hasResource = whoHasAccess.getOrDefault(resource, null);
					assert hasResource != null;
					
					synchronized (deadlockResolveLock) {
						currentTr.setWaitFor(hasResource);
						
						Transaction newest = currentTr;
						Transaction nextTr = hasResource;
						
						while (nextTr != null && nextTr != currentTr) {
							if (nextTr.compareTo(newest) > 0)
								newest = nextTr;
							
							nextTr = nextTr.getNext();
						}
						
						if (nextTr == currentTr) {
							// deadlock found
							newest.resolveDeadlock();
						}
					}
					
					Queue<Transaction> rQueue = whoWaitsOnResource.getOrDefault(resource, null);
					assert rQueue != null;
					
					rQueue.add(currentTr);
					currentTr.lock();
					
					resource.wait();
				}
				
				whoHasAccess.replace(resource, currentTr);
				currentTr.addResource(resource);
			}
		}
		
		operation.execute(resource);
		currentTr.registerOperation(operation, resource);
	}
	
	private void unlockResource(Resource r) {
		synchronized (r) {
			whoHasAccess.replace(r, dummyTransaction);
			
			Queue<Transaction> rQueue = whoWaitsOnResource.getOrDefault(r, null);
			assert rQueue != null;
			
			if (!rQueue.isEmpty()) {
				Transaction oldestTr = rQueue.remove();
				
				synchronized (deadlockResolveLock) {
					oldestTr.setWaitFor(null);
				}
				
				oldestTr.unlock();
				r.notifyAll();
			}
		}
	}
	
	private void unlockCurrentResources() {
		Transaction currentTr = tryGetActiveTransaction();
		Iterator<Resource> it = currentTr.getResourcesIterator();
		// tutaj i tam wyzej w unlockresource, mozliwe ze trzeba bedzie zalozyc locka na transakcje
		// wynika to z tego, ze zmieniamy kolekcje resourcow. no ale moze iterator ogarnie, nie wiem
		// raczej nie
		
		while (it.hasNext()) {
			Resource r = it.next();
			unlockResource(r);
		}
	}
	
	private void removeActiveTransaction() {
		Transaction currentTr = tryGetActiveTransaction();
		activeTransactions.remove(Thread.currentThread(), currentTr);
	}
	
	@Override
	public void commitCurrentTransaction(
	) throws
		NoActiveTransactionException,
		ActiveTransactionAborted {
		
		if (!isTransactionActive())
			throw new NoActiveTransactionException();
		if (isTransactionAborted())
			throw new ActiveTransactionAborted();
		
		unlockCurrentResources();
		removeActiveTransaction();
	}
	
	@Override
	public void rollbackCurrentTransaction() {
		Transaction currentTr = tryGetActiveTransaction();
		
		if (currentTr == null) {
			return;
		}
		
		while (!currentTr.isStackTraceEmpty()) {
			Pair<ResourceOperation, Resource> trace = currentTr.stackTracePop();
			
			ResourceOperation operation = trace.first();
			Resource resource = trace.second();
			
			operation.undo(resource);
			
			if (currentTr.rollbackResource(resource))
				unlockResource(resource);
		}
		
		removeActiveTransaction();
	}
	
	private Transaction tryGetActiveTransaction() {
		Thread current = Thread.currentThread();
		return activeTransactions.getOrDefault(current, null);
	}
	
	@Override
	public boolean isTransactionActive() {
		boolean result = false;
		Transaction tr = tryGetActiveTransaction();
		
		if (tr != null) {
			result = true;
		}
		
		return result;
	}
	
	@Override
	public boolean isTransactionAborted() {
		boolean result = false;
		Transaction tr = tryGetActiveTransaction();
		
		if (tr != null && tr.isFlgAborted()) {
			result = true;
		}
		
		return result;
	}
	
}
