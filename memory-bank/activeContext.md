# Active Context

## Current focus

- Local external controllers for embedded TUI launches are implemented and
  validated through five live launches.
- Approved controllers subscribe to the immutable TUI main-thread listener,
  acquire a single lease for mutations, and release control back to the TUI.
- Local-controller startup prunes artifacts for definitely dead processes while
  preserving live or ambiguous records.
- A disconnected controller cannot receive a late native approval or retain
  ownership.

## Immediate follow-up

- Keep the V7 downstream device status-only; physical tap input is a separate
  product capability, not an external-controller acceptance criterion.
- Re-run focused controller and local-controller tests after behavioral changes.

See [the implementation plan](../docs/external-controllers-implementation-plan.md)
for design details and `progress.md` for milestone status.
