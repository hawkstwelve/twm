## Artifact Contract

### RGBA tile artifact (per model/region/run/var/fh)

| Property | Value |
|---|---|
| CRS | EPSG:3857 |
| Bands | 4 (R, G, B, A), uint8 |
| Alpha | 0 = nodata/outside; 255 = valid data |
| Internal tiling | 512×512 |
| Overviews | Internal (see locked overview strategy below) |
| Filename | `fh{NNN}.rgba.cog.tif` |

#### Locked Overview Strategy

Overview resampling is defined exactly twice — once for continuous, once for categorical — and encoded in `VarSpec.kind`. There are no per-variable overrides, no special cases, no hacks.
This policy applies globally across all models (including HRRR and GFS).

| `VarSpec.kind` | Bands 1–3 (RGB) | Band 4 (Alpha) | `gdaladdo` flags |
|---|---|---|---|
| `continuous` | `average` | `nearest` | Two-pass `gdaladdo` on one 4-band GTiff: pass 1 `-r nearest -b 4` (alpha), pass 2 `-r average -b 1 -b 2 -b 3` (RGB), then COG translate |
| `discrete` | `nearest` | `nearest` | `-r nearest` for all bands |

**Rules:**

1. The builder reads `VarSpec.kind` and selects the corresponding row. No other input affects overview resampling.
2. There is no `overview_resampling` field on `VarSpec`. The two strategies above are the only ones that exist.
3. If a variable produces visually incorrect overviews, the fix is to change its `kind` classification (continuous ↔ discrete), not to add a per-variable override.
4. Alpha is always `nearest` — never averaged, interpolated, or thresholded. This is non-negotiable.
5. This strategy is locked at Phase 0 and not revisited unless there is a measured, reproducible visual defect with evidence attached.

This eliminates the historical pain of per-variable overview hacks and keeps overview behavior deterministic across models.

### Value-grid artifact (per model/region/run/var/fh)

| Property | Value |
|---|---|
| CRS | EPSG:3857 (same extent as RGBA; may use coarser hover grid) |
| Bands | 1, float32 |
| Nodata | Explicitly set (NaN or sentinel per var spec) |
| Overviews | Internal, nearest resampling |
| Filename | `fh{NNN}.val.cog.tif` |

Current production optimization for hover sampling uses a 4x coarser value grid
(pixel size ×4, ~1/16 pixel count). RGBA remains full-resolution.

### Sidecar metadata (per frame)

Filename: `fh{NNN}.json`

```json
{
  "contract_version": "3.0",
  "model": "hrrr",
  "region": "conus",
  "run": "20260217_06z",
  "var": "tmp2m",
  "fh": 3,
  "valid_time": "2026-02-17T09:00:00Z",
  "units": "°F",
  "kind": "continuous",
  "min": -40.0,
  "max": 122.5,
  "legend": {
    "type": "gradient",
    "stops": [[-40, "#7f00ff"], [0, "#0000ff"], [32, "#00ffff"], [70, "#ffff00"], [100, "#ff0000"], [122.5, "#8b0000"]]
  }
}
```

For categorical variables, `legend.type` is `"discrete"` with named category stops.

#### Sidecar immutability note

Legend metadata is frame-scoped and immutable once published. The frontend legend is sourced from each frame's sidecar (`fh{NNN}.json`), so palette/anchor changes in code only appear for newly built artifacts. Existing published runs/frames retain their original legend metadata until those frames are rebuilt and republished.

---
