package dev.gaphunter.kafkaprematureoffsetcommitcompanion.model

import com.intellij.psi.PsiElement

/** One Kafka consumer poll-loop that dispatches async work and either commits immediately or relies on default auto-commit -- either way, without waiting for the async work to actually finish. */
data class PrematureCommitHit(val anchor: PsiElement, val hasExplicitCommit: Boolean)
