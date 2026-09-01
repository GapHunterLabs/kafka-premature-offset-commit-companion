package dev.gaphunter.kafkaprematureoffsetcommitcompanion.detect

import com.intellij.psi.JavaRecursiveElementWalkingVisitor
import com.intellij.psi.PsiDoWhileStatement
import com.intellij.psi.PsiElement
import com.intellij.psi.PsiFile
import com.intellij.psi.PsiForStatement
import com.intellij.psi.PsiLoopStatement
import com.intellij.psi.PsiMethodCallExpression
import com.intellij.psi.PsiWhileStatement
import dev.gaphunter.kafkaprematureoffsetcommitcompanion.model.PrematureCommitHit

/**
 * Finds a Kafka consumer poll-loop (`while (true) { ... poll(...) ... }`)
 * whose body dispatches asynchronous work (`ExecutorService.submit(...)`,
 * `CompletableFuture.runAsync(...)`/`.supplyAsync(...)`) without waiting
 * for it to finish (`.get()`/`.join()`/`.awaitTermination()`) BEFORE the
 * loop commits offsets -- either explicitly (`commitSync()`/
 * `commitAsync()` called right there, regardless of the async work's
 * real completion) or implicitly (no manual commit at all, relying on
 * Kafka's own default `enable.auto.commit=true`, which commits on the
 * next `poll()` cycle regardless of whether the dispatched work
 * finished). If the consumer crashes between the commit and the real
 * completion of that work, the message is lost silently -- no error,
 * no retry, nothing visible.
 *
 * **Correlates three independent signals in the same loop body** --
 * none alone is enough: async dispatch alone is fine if the loop waits
 * for it; a commit call alone is fine if nothing async was dispatched;
 * and there being no explicit commit at all only matters because it
 * means auto-commit (the risky default) is in play.
 *
 * **v0.1 scope, stated honestly:** only the official
 * `org.apache.kafka:kafka-clients` client with the standard poll-loop
 * shape -- never covers Kafka Streams or wrapped messaging frameworks
 * (Spring Kafka's `@KafkaListener` is out of scope, a future
 * extension).
 */
object JavaPrematureCommitFinder {

    private val ASYNC_DISPATCH_METHODS = setOf("submit", "runAsync", "supplyAsync")
    private val WAIT_METHODS = setOf("get", "join", "awaitTermination")
    private val COMMIT_METHODS = setOf("commitSync", "commitAsync")

    fun findAll(file: PsiFile): List<PrematureCommitHit> {
        val hits = mutableListOf<PrematureCommitHit>()
        // No generic "visitLoopStatement" exists on this visitor -- the
        // platform only exposes per-concrete-type overrides, so each real
        // loop shape (while/for/do-while) is handled explicitly.
        file.accept(object : JavaRecursiveElementWalkingVisitor() {
            override fun visitWhileStatement(statement: PsiWhileStatement) {
                super.visitWhileStatement(statement)
                hitForLoop(statement)?.let { hits += it }
            }

            override fun visitDoWhileStatement(statement: PsiDoWhileStatement) {
                super.visitDoWhileStatement(statement)
                hitForLoop(statement)?.let { hits += it }
            }

            override fun visitForStatement(statement: PsiForStatement) {
                super.visitForStatement(statement)
                hitForLoop(statement)?.let { hits += it }
            }
        })
        return hits
    }

    private fun hitForLoop(loop: PsiLoopStatement): PrematureCommitHit? {
        val body = loop.body ?: return null
        if (!containsCallNamed(body, setOf("poll"))) return null // not a consumer poll-loop at all

        val asyncDispatchCall = firstCallNamed(body, ASYNC_DISPATCH_METHODS) ?: return null
        if (containsCallNamed(body, WAIT_METHODS)) return null // waits synchronously -- safe, regardless of commit shape

        val hasExplicitCommit = containsCallNamed(body, COMMIT_METHODS)
        return PrematureCommitHit(anchorOf(asyncDispatchCall), hasExplicitCommit)
    }

    private fun containsCallNamed(scope: PsiElement, names: Set<String>): Boolean = firstCallNamed(scope, names) != null

    private fun firstCallNamed(scope: PsiElement, names: Set<String>): PsiMethodCallExpression? {
        var found: PsiMethodCallExpression? = null
        scope.accept(object : JavaRecursiveElementWalkingVisitor() {
            override fun visitMethodCallExpression(expression: PsiMethodCallExpression) {
                if (found != null) return
                super.visitMethodCallExpression(expression)
                if (expression.methodExpression.referenceName in names) found = expression
            }
        })
        return found
    }

    private fun anchorOf(call: PsiMethodCallExpression): PsiElement =
        call.methodExpression.referenceNameElement ?: call.methodExpression
}
