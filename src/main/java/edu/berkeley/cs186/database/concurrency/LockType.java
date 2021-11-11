package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a.equals(LockType.NL) || b.equals(LockType.NL)) {
            return true;
        }
        if (a.equals(LockType.IS)) {
            return !b.equals(LockType.X);
        }
        if (a.equals(LockType.IX)) {
            return b.equals(LockType.IS) || b.equals(LockType.IX);
        }
        if (a.equals(LockType.S)) {
            return b.equals(LockType.IS) || b.equals(LockType.S);
        }
        if (a.equals(LockType.SIX)) {
            return b.equals(LockType.IS);
        }
        if (a.equals(LockType.X)) {
            return false;
        } else {
            return false;
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (parentLockType.equals(NL)) {
            return childLockType.equals(NL);
        }
        if (childLockType.equals(NL)) {
            return true;
        }
        if (parentLockType.equals(IS)) {
            return childLockType.equals(IS) || childLockType.equals(S) || childLockType.equals(NL);
        }
        if (parentLockType.equals(IX)) {
            return true;
        }
        if (parentLockType.equals(S) || parentLockType.equals(X)) {
            return false;
        }
        if (parentLockType.equals(SIX)) {
            return childLockType.equals(X) || childLockType.equals(IX);
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (required.equals(NL)) {
            return true;
        }
        if (required.equals(IS)) {
            return substitute.equals(IS) || substitute.equals(IX);
        }
        if (required.equals(IX)) {
            return substitute.equals(IX) || substitute.equals(SIX);
        }
        if (required.equals(S)) {
            return substitute.equals(S) || substitute.equals(SIX) || substitute.equals(X);
        }
        if (required.equals(X)) {
            return substitute.equals(X);
        }
        if (required.equals(SIX)) {
            return substitute.equals(SIX);
        }
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

