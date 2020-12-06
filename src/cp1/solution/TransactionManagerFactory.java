/*
 * University of Warsaw
 * Concurrent Programming Course 2020/2021
 * Java Assignment
 * 
 * Author: Konrad Iwanicki (iwanicki@mimuw.edu.pl)
 * Author: Rafal Klimek (rk418291@students.mimuw.edu.pl)
 */
package cp1.solution;

import java.util.Collection;

import cp1.base.TransactionManager;
import cp1.base.LocalTimeProvider;
import cp1.base.Resource;

/**
 * A factory for instantiating transaction managers.
 * 
 * @author Konrad Iwanicki (iwanicki@mimuw.edu.pl)
 * @author Rafal Klimek (rk418291@students.mimuw.edu.pl)
 */
public final class TransactionManagerFactory {
	
	/**
	 * Instantiates your solution: returns a
	 * new transaction manager that takes over
	 * control over a given collection of resources,
	 * to which end it uses local time as
	 * offered by a given provider.
	 * @param resources The collection of resources.
	 * @param timeProvider A local time provider.
	 * @return A new transaction manager for
	 *     controlling the resources.
	 */
	public final static TransactionManager newTM(
			Collection<Resource> resources,
			LocalTimeProvider timeProvider
	) {
		return new MT(resources, timeProvider);
	}
	
}
