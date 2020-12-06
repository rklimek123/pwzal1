/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Rafal Klimek (rk418291@students.mimuw.edu.pl)
 */
package cp1.solution;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

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

	private final ConcurrentMap<Thread, Transaction> activeTransactions;
	private final ConcurrentMap<Resource, Transaction> whoHasAccess;
	
	// Taken when modifying the structure of a waiting graph
	private final Object deadlockResolveLock = new Object();
	
	MT(
			Collection<Resource> resources,
			LocalTimeProvider timeProvider
	) {
		this.resources = resources;
		this.timeProvider = timeProvider;
		
		activeTransactions = new ConcurrentHashMap<Thread, Transaction>();
		whoHasAccess = new ConcurrentHashMap<Resource, Transaction>();
		
		for (Resource r: resources) {
			whoHasAccess.putIfAbsent(r, null);
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
		
		if (!currentTr.resourcesInUse.contains(resource)) {
			synchronized (resource) {
				while ((Transaction hasResource = whoHasAccess.getOrDefault(resource, null)) != null) {
					synchronized (deadlockResolveLock) {
						currentTr.setWaitFor(hasResource);
						
						Transaction newest = currentTr;
						Transaction nextTr = hasResource;
						
						while (nextTr != null || nextTr != currentTr) {
							if (nextTr.isNewerThan(newest))
								newest = nextTr;
							
							nextTr = nextTr.getNext();
						}
						
						if (nextTr == currentTr) {
							// deadlock found
							newest.resolveDeadlock();
						}
					}
					
					resource.wait();
					// or should changing waitingFor on currentTr happen when notifying the ones
					// waiting for that resource? then we'd need some priority queue sorted by startingDate()
					// to notify the oldest Transaction, and then to correct the connections in this Transaction.
					// Analiza przeplotu potrzebna, czy naprawdę trzeba to robić??
					
				}
				
				
				whoHasAccess.replace(resource, currentTr);
				currentTr.resourcesInUse.add(resource);
			}
		}
		
		operation.execute(resource);
		currentTr.stackTrace.add(new Pair<ResourceOperation, Resource>(operation, resource));
		
	}
	
	private void freeResource(Resource r) {
		synchronized (r) {
			whoHasAccess.replace(r, null);
			r.notify();
		}
	}
	
	private void freeCurrentResources() {
		Transaction currentTr = tryGetActiveTransaction();
		
		for (Resource r: currentTr.resourcesInUse) {
			freeResource(r);
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
		
		freeCurrentResources();
		removeActiveTransaction();
	}
	
	@Override
	public void rollbackCurrentTransaction() {
		Transaction currentTr = tryGetActiveTransaction();
		
		while (!(currentTr.stackTrace.empty())) {
			Pair<ResourceOperation, Resource> trace = currentTr.stackTrace.pop();
			
			ResourceOperation operation = trace.first();
			Resource resource = trace.second();
			
			operation.undo(resource);
			
			//freeResource(resource); /////// TUTAJ ZROBIC TRZEBA JAKIS LICZNIK ILE RAZY UZYTA TA
									// ZMIENNA, A POTEM JAK OSTATNIA JEST ROLLBACKOWANA TO WTEDY
									// DOPIERO ZWALNIAM TEN ZASOB
		}
		
		freeCurrentResources(); // ROZWIAZANIE NA PALE, TRZEBA ZROBIC TAK JAK WYZEJ
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
		
		if (tr != null && tr.isAborted) {
			result = true;
		}
		
		return result;
	}
	
}
