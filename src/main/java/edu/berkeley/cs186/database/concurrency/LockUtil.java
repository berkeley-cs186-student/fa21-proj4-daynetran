package edu.berkeley.cs186.database.concurrency;

import java.net.http.HttpClient.Redirect;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor locks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        
        // We will break up cases based on what requestType is to be: S, X, NL

        // 1) ensure all ancestors have either IS, IX, or S (If the transaction already holds S or SIX,
        //  then it is redundant to acquire a S lock on the current resource because we already have the requested permissions on it)
        // 2) change current lock to S
        // 3)
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        
        if (requestType == LockType.S) {
            // 1. do all valid operations on the current resource
            // lockContext.escalate(transaction);
            // 2. get appropriate locks on ancestors
            if (lockContext.parentContext() != null) {
                updateAncestorByS(lockContext.parentContext(), transaction);
            }
            // 3. do all remaining operations that weren't valid at 1.
            if (explicitLockType == LockType.NL) {
                lockContext.acquire(transaction, LockType.S);
            } else {
                lockContext.promote(transaction, LockType.S);
            }
            
        }
        // Case 2: X
        else if (requestType == LockType.X) {
            // 1. do all valid operations on the current resource
            if (explicitLockType == LockType.IX || explicitLockType == LockType.IS) {
                lockContext.escalate(transaction); // if this resource has an intent lock, then we will escalate to remove descendant locks
            }
            // 2. get appropriate locks on ancestors
            if (lockContext.parent != null) {
                updateAncestorsByX(lockContext.parentContext(), transaction);
            }
            // 3. do all remaining operations that weren't valid at 1.
            lockContext.promote(transaction, requestType);
        }        
    }

    // TODO(proj4_part2) add any helper methods you want
    private static void updateAncestorByS(LockContext ancestor, TransactionContext transaction) {
        // if ancestor is IX IS S, leave as is 
        // if ancestor is X, promote to SIX
        // if ancestor is SIX, promote to IX
        // if ancestor is NL, acquire IS
        ArrayDeque<LockContext> ancestors = new ArrayDeque<>();
        boolean hasParent = true;
        while (hasParent) {
            ancestors.add(ancestor);
            if (ancestor.parent == null) {
                hasParent = false;
            } else {
                ancestor = ancestor.parentContext();
            }
        }
        while (!ancestors.isEmpty()) {
            ancestor = ancestors.removeLast();
            if (ancestor.getExplicitLockType(transaction) == LockType.NL) {
                ancestor.acquire(transaction, LockType.IS);
            }
            if (ancestor.getExplicitLockType(transaction) == LockType.IX) {
                ancestor.promote(transaction, LockType.SIX);
            }
        }
        


    }
    private static void updateAncestorsByX(LockContext ancestor, TransactionContext transaction) {
        // if ancestor is X SIX IX, leave as is 
        // if ancestor is S, promote to SIX
        // if ancestor is IS or NL, promote to IX
        boolean hasParent = true;
        while (hasParent) {
            if (ancestor.getExplicitLockType(transaction) == LockType.S) {
                ancestor.promote(transaction, LockType.SIX);
            } else if (ancestor.getExplicitLockType(transaction) == LockType.IS) {
                ancestor.promote(transaction, LockType.IX);
            } else if (ancestor.getExplicitLockType(transaction) == LockType.NL) {
                ancestor.acquire(transaction, LockType.IX);
            }
            if (ancestor.parent == null) {
                hasParent = false;
            } else {
                ancestor = ancestor.parentContext();
            }
        }
    }
}
