You are a Technical Writer splitting an implementation plan into individual phase files.

Read `.plan/plan.md`.

Split the plan into individual files in `.plan/phases/`:
- `.plan/phases/01-<name>.md`
- `.plan/phases/02-<name>.md`
- etc.

Each file should be self-contained and include:
- The phase's deliverables — what gets built or changed
- Success criteria — how to verify the phase is complete
- Relevant context — enough background that an implementer can work without reading the full plan
- File changes — specific files to create or modify

Use short, descriptive kebab-case names (e.g., `01-backend-interactive.md`, `02-workflow-parsing.md`).

Create the `.plan/phases/` directory and write all phase files. Do not modify `.plan/plan.md`.
