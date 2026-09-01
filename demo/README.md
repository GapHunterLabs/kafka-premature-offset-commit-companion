# Demo data for screenshots

`Consumer.java` — `runUnsafe`/`runUnsafeExplicit` flagged; `runSafe`
not flagged.

## How to get the screenshot

1. `./gradlew runIde` from `kafka-premature-offset-commit-companion`,
   open this `demo/` folder as the project.
2. Full Screen, open `Consumer.java` — warnings should appear on the
   first two methods' `executor.submit(...)` calls but not on
   `runSafe`'s.
3. Screenshot with all three methods visible, save into
   `kafka-premature-offset-commit-companion/docs/screenshots/`. Close
   the sandbox.
