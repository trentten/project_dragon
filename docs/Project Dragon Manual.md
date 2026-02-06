Project Dragon — Agent Operating Manual
Team Roles
Trent (Founder)

Sets product vision

Approves scope

Sets priorities

Final decision maker

Drew (Analyst)

Translates ideas into specs

Designs systems

Prevents architectural mistakes

Prepares Codex-ready handoffs

Codex (Engineer)

Implements features

Maintains stability

Writes production code

Optimizes performance

Standard Workflow
IDEA → DREW → SPEC → CODEX → IMPLEMENT → TEST → DEPLOY

Drew → Codex Handoff Format

All work must be handed off using:

## TASK HANDOFF — Project Dragon

Feature Name:

Priority:

Summary:

Scope:
- Included
- Excluded

Dependencies:
- Modules
- Tables
- UI pages

Acceptance Criteria:
- Pass/fail list

Implementation Notes:
- Technical constraints
- Performance considerations
- Migration needs

Docs Reference:
- /docs/[file]

Documentation Authority

/docs is the source of truth

Architecture standards must be followed

Changes must stay consistent with documented patterns

Docs must be updated if behavior changes

Docs Inventory (current)

Available:
- Project Dragon Manual.md
- db_tools.md
- project_dragon_plan_v0.1.0.md
- project_dragon_plan_v0.2.0.md
- smoke_tests.md
- ui_icons.md

Missing (referenced in some handoffs):
- architecture.md
- ui-standards.md

Action: update handoffs to reference available docs until missing docs are added.

If a task touches multiple subsystems:

Required additions:

Impact Summary

Migration Plan

Rollback Strategy

Performance Impact Notes