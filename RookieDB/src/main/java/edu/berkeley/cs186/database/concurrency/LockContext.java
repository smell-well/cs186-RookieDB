package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.query.disk.Partition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    public void updateChildLockNum(TransactionContext transaction, int delta) {
        int lockNum = getNumChildren(transaction);
        lockNum += delta;
        numChildLocks.put(transaction.getTransNum(), lockNum);
        LockContext parent = parentContext();
        if (parent != null) {
            parent.updateChildLockNum(transaction, delta);
        }
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        if (this.readonly) {
            throw new UnsupportedOperationException();
        }
        LockContext parentCTX = parentContext();
        if (parentCTX != null && !LockType.canBeParentLock(parentCTX.getEffectiveLockType(transaction), lockType)) {
            throw new InvalidLockException("the request is invalid");
        }
        lockman.acquire(transaction, getResourceName(), lockType);
        if (parentCTX != null) {
            // update numChildLocks
            parentCTX.updateChildLockNum(transaction, 1);
        }

    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (this.readonly) {
            throw new UnsupportedOperationException();
        }

        // 判断 后代 是否还有锁
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("would violate multigranularity locking constraints");
        }

        lockman.release(transaction, getResourceName());
        LockContext parentCTX = parentContext();
        if (parentCTX != null) {
            parentCTX.updateChildLockNum(transaction, -1);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (this.readonly) {
            throw new UnsupportedOperationException("context is readonly");
        }
        // 在 SIX 下不能升级成为S、IS、SIX
        if (hasSIXAncestor(transaction) && ((newLockType == LockType.S || newLockType == LockType.IS
            || newLockType == LockType.SIX))) {
            throw new InvalidLockException("conflict SIX");
        }
        // 判断当前树能否兼容newLockType
        // S + IX 升级成 SIX 锁
        if (this.parent != null) {
            LockContext parentCTX = parentContext();
            LockType parentHeld = parentCTX.getEffectiveLockType(transaction);
            if (!LockType.canBeParentLock(parentHeld, newLockType)) {
                throw new InvalidLockException("conflict with parent locks");
            }
        }
        // 尝试升级
        if (newLockType != LockType.SIX) {
            // 还存隐式升级成SIX的情况
            LockType held = getExplicitLockType(transaction);
            if (held == LockType.S && newLockType == LockType.IX) {
                newLockType = LockType.SIX;
            }
            lockman.promote(transaction, name, newLockType);
        } else {
            List<ResourceName> needToRelease = sisDescendants(transaction);
            // 加上自己
            needToRelease.add(name);
            LockType held = getExplicitLockType(transaction);
            if (held == LockType.NL) {
                throw new NoLockHeldException("transaction no lock held");
            }
            if (held == newLockType) {
                throw new DuplicateLockRequestException("duplicate lock require");
            }
            if (!LockType.substitutable(newLockType, held)) {
                throw new InvalidLockException("newLockType not substitute heldLockType");
            }
            lockman.acquireAndRelease(transaction, name, newLockType, needToRelease);
            // 对于每一个释放的锁，都要修改锁的计数
            for (ResourceName rname : needToRelease) {
                LockContext ctx = fromResourceName(lockman, rname);
                ctx.updateChildLockNum(transaction, -1);
            }
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (this.readonly) {
            throw new UnsupportedOperationException("context is readonly");
        }
        LockType held = getEffectiveLockType(transaction);
        if (held == LockType.NL) {
            throw new NoLockHeldException("no lock at this level");
        }
        List<ResourceName> desc = new ArrayList<>();
        // 把自己加入
        desc.add(name);
        List<Lock> locks = lockman.getLocks(transaction);
        boolean allSIX = held == LockType.S || held == LockType.IS;
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name)) {
                if (!(lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                    allSIX = false;
                }
                desc.add(lock.name);
            }
        }
        LockType newLockType = LockType.X;
        if (allSIX) {
            newLockType = LockType.S;
        }
        if (newLockType != held) {
            // 需要升级
            lockman.acquireAndRelease(transaction, name, newLockType, desc);
            for (ResourceName rname : desc) {
                LockContext ctx = fromResourceName(lockman, rname);
                if (ctx.parent != null) {
                    ctx.parent.updateChildLockNum(transaction, -1);
                }
            }
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        LockType curr = getExplicitLockType(transaction);
        if (curr != LockType.NL) return curr;
        LockContext parentCTX = parentContext();
        if (parentCTX != null) {
            LockType parentLockType = parentCTX.getEffectiveLockType(transaction);
            if (parentLockType == LockType.SIX) {
                return LockType.S;
            } else if (!parentLockType.isIntent()) {
                return parentLockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext ancestorCTX = parentContext();
        while (ancestorCTX != null) {
            if (lockman.getLockType(transaction, ancestorCTX.getResourceName()) == LockType.SIX) {
                return true;
            }
            ancestorCTX = ancestorCTX.parentContext();
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<ResourceName> res = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(getResourceName())
                    && (lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                res.add(lock.name);
            }
        }
        return res;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

