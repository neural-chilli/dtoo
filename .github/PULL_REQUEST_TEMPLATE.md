<!--
BEFORE SUBMITTING: Read every word of this template. PRs that leave
sections blank, contain multiple unrelated changes, or show no evidence
of human involvement will be closed without review.
-->

## What problem are you trying to solve?
<!-- Describe the specific problem you encountered. What broke? What
     failed? What was the user experience that motivated this?

     "Improving" something is not a problem statement. Describe the
     specific error, incorrect output, or missing capability. -->

## What does this PR change?
<!-- 1-3 sentences. What, not why — the "why" belongs above. -->

## Does this change align with DESIGN.md?
<!-- dtoo follows a specific architecture and pipeline design. Confirm:

     - Does this change respect the pipeline execution order?
     - Does it follow the CLI conventions (data to stdout, logs to stderr)?
     - Does it use the correct exit codes?
     - If this touches a Pro/Enterprise feature, is it behind the
       appropriate licensing tier?

     If your change deviates from DESIGN.md, explain why and confirm
     you've discussed it in an issue first. -->

## What alternatives did you consider?
<!-- What other approaches did you try or evaluate before landing on this
     one? Why were they worse? If you didn't consider alternatives, say so
     — but know that's a red flag. -->

## Does this PR contain multiple unrelated changes?
<!-- If yes: stop. Split it into separate PRs. Bundled PRs will be closed.
     If you believe the changes are related, explain the dependency. -->

## Existing PRs
- [ ] I have reviewed all open AND closed PRs for duplicates or prior art
- Related PRs: <!-- #number, #number, or "none found" -->

<!-- If a related closed PR exists, explain what's different about your
     approach and why it should succeed where the other didn't. -->

## Testing
- [ ] `cargo test` passes
- [ ] `cargo clippy` passes with no warnings
- [ ] `cargo fmt` has been run
- New tests added: <!-- Describe what tests you added and what they cover -->

<!-- If you didn't add tests, explain why. "It's a small change" is not
     a valid reason — small changes can still break things. -->

## Evaluation
- What was the specific scenario you tested?
- What was the output before and after the change?
- Did you test error cases (bad input, missing files, invalid SQL)?

<!-- "It works" is not evaluation. Show specific before/after output. -->

## Human review
- [ ] A human has reviewed the COMPLETE proposed diff before submission

<!--
STOP. If the checkbox above is not checked, do not submit this PR.

PRs will be closed without review if they:
- Show no evidence of human involvement
- Contain multiple unrelated changes
- Leave required sections blank or use placeholder text
- Break the pipeline design described in DESIGN.md
- Add unnecessary dependencies without justification
-->
