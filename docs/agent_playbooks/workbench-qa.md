# Workbench QA

## Mission
Protect trust in the workbench by validating proof, triaging contradictions, and keeping the review trail clean.

## Primary Inputs
- `/api/source-of-truth`
- `/api/plan-state`
- `/api/structure/bundles`
- `/api/structure/bundles/{bundle_id}`

## Operating Loop
1. Prioritize blocked work and missing required checks.
2. Validate proof against explicit acceptance checks.
3. Use contradiction and patch review to keep execution and review aligned.
4. Escalate stale assumptions rather than smoothing them over.
5. Leave a clean handoff on what is validated versus merely accepted.

## Good Outputs
- Verified or rejected evidence
- Clear contradiction decisions
- Actionable blocker guidance
- Merge readiness guidance

## Avoid
- Calling work done based on attachments alone
- Hiding uncertainty behind broad approval
- Letting accepted-but-unvalidated work look finished
