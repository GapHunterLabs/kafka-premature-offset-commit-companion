package dev.gaphunter.kafkaprematureoffsetcommitcompanion.inspection

import com.intellij.codeInspection.InspectionManager
import com.intellij.codeInspection.LocalInspectionTool
import com.intellij.codeInspection.ProblemDescriptor
import com.intellij.codeInspection.ProblemHighlightType
import com.intellij.psi.PsiFile
import com.intellij.psi.PsiJavaFile
import dev.gaphunter.kafkaprematureoffsetcommitcompanion.detect.JavaPrematureCommitFinder
import dev.gaphunter.kafkaprematureoffsetcommitcompanion.model.PrematureCommitHit
import dev.gaphunter.kafkaprematureoffsetcommitcompanion.review.ReviewPrompt

/**
 * Flags a Kafka consumer poll-loop that dispatches asynchronous work
 * without waiting for it to finish before committing offsets (either
 * explicitly or via the default auto-commit) -- if the consumer
 * crashes between the commit and the real completion of that work,
 * the message is lost silently. Runs via `checkFile` (same shape as
 * every other inspection in this catalog);
 * [JavaPrematureCommitFinder] does the real PSI walk.
 */
class PrematureCommitInspection : LocalInspectionTool() {

    companion object {
        const val MAX_FILE_LENGTH = 500_000
    }

    override fun checkFile(file: PsiFile, manager: InspectionManager, isOnTheFly: Boolean): Array<ProblemDescriptor>? {
        if (file.text.length > MAX_FILE_LENGTH) return null
        if (file !is PsiJavaFile) return null

        val hits = JavaPrematureCommitFinder.findAll(file)
        if (hits.isEmpty()) return null

        val problems = hits.map { hit ->
            manager.createProblemDescriptor(
                hit.anchor,
                messageFor(hit),
                isOnTheFly,
                emptyArray(),
                ProblemHighlightType.GENERIC_ERROR_OR_WARNING,
            )
        }

        val path = file.virtualFile?.path
        if (path != null) {
            for (hit in hits) {
                val lineNumber = file.viewProvider.document?.getLineNumber(hit.anchor.textRange.startOffset) ?: -1
                ReviewPrompt.recordHit(file.project, "$path:$lineNumber")
            }
        }

        return problems.toTypedArray()
    }

    private fun messageFor(hit: PrematureCommitHit): String {
        val commitDetail = if (hit.hasExplicitCommit) {
            "commits immediately after dispatching this async work, without waiting for it to finish"
        } else {
            "relies on the default enable.auto.commit=true, which commits on the next poll() regardless of " +
                "whether this async work finished"
        }
        return "Async dispatch inside a Kafka poll-loop, and the consumer $commitDetail -- if it crashes before " +
            "the work completes, the message is lost silently, with no error or retry"
    }
}
