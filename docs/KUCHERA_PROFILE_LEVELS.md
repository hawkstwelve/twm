# Kuchera Profile Levels Note

This note captures the current operational profile-level choice for
`snowfall_kuchera_total` and the intended rollout guidance for future models.

## Current Baseline

- Shared default Kuchera level set remains `925,850,700,600,500`.
- HRRR currently runs Kuchera in simplified mode with the configured level set
  `925,850,700,600`.
- Simplified mode is still capped at four temperature-profile fetches per step.

## Why HRRR Uses 925/850/700/600

- `925 mb` is included to better catch shallow warm layers and warm noses that
  can otherwise push Kuchera SLR too high in marginal snow setups.
- `500 mb` is dropped in simplified mode to keep the same per-step fetch count
  and preserve derive performance.
- This is a configuration choice for HRRR, not a fork of the shared Kuchera
  derive path.

## Rollout Guidance For GFS And NAM

- If a model is enabled with the full Kuchera profile, start from the shared
  default set `925,850,700,600,500`.
- If a model needs simplified-mode performance, prefer evaluating
  `925,850,700,600` before falling back to `850,700,600,500`.
- The main reason is operational: keeping `925 mb` in the simplified set helps
  detect shallow warm layers without increasing the number of temperature
  fetches.
- If a future model verifies cold-biased or underdone with `925 mb` included,
  adjust the configured level set per model rather than branching the shared
  Kuchera algorithm.

## Practical Interpretation

- `925 mb` inclusion is an accuracy-oriented swap, not an added performance
  cost.
- Future rollout decisions for GFS and NAM should treat the level set as a
  model-specific configuration knob layered on top of the common Kuchera derive
  implementation.