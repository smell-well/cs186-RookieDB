package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.query.disk.Partition;

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
     *   lock type can be, and think about how ancestor looks will need to be
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
        // The current lock type can effectively substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            // 此时应该申请 SIX 锁
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // 如果 curr lock 是intent lock
        // 用explicit 判断是否是意图锁可以说明是否在叶子节点
        if (explicitLockType.isIntent()) {
            // 因为不是真正的锁级别，所以需要获得当前后代所真正需要的锁级别
            lockContext.escalate(transaction);
            // 当前锁只能是 S， X
            LockType afterEscalateLockType = lockContext.getEffectiveLockType(transaction);

            // 这两种情况是权限是大于要求的所以可以直接跳过
            if (afterEscalateLockType == requestType ||requestType == LockType.S) {
                return;
            }
        }

//        if (effectiveLockType != LockType.NL) {
//            // 当前实际上是 S 锁
//            if (requestType == LockType.X) {
//                lockContext.promote(transaction, requestType);
//                if (parentContext != null) {
//                    parentContext.escalate(transaction);
//                }
//                return;
//            }
//        }

        // 此时，当前 level 的锁 只能是 S, NL, (x 在 substitutable 被过滤)
        // 所有的可能性为 request, currLock: (S, NL), (X, NL), (X, S)
        if (requestType == LockType.S) {
            // 需要确保祖先都加上了 意向锁
            ensureAncestor(LockType.IS, parentContext, transaction);
        } else {
            // current is NL(effective S) requst is X
            ensureAncestor(LockType.IX, parentContext, transaction);
        }

        // 已经添加了意向锁，需要确定真正的锁等级
        if (explicitLockType == LockType.NL) {
            // 第一次获取锁
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }

    }


    public static void ensureAncestor(LockType lockType, LockContext current, TransactionContext transaction) {
        if (current != null) {
            // current level lockType
            ensureAncestor(lockType, current.parentContext(), transaction);
            LockType currLockType = current.getExplicitLockType(transaction);
            // 不能被兼容，所以需要加锁或者升级
            if (!LockType.substitutable(currLockType, lockType)) {
                if (currLockType == LockType.NL) {
                    current.acquire(transaction, lockType);
                } else {
                    current.promote(transaction, lockType);
                }
            }
        }
    }
}
