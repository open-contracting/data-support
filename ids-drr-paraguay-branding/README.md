# IDS-DRR: Paraguay branding

Demo branding package for the Paraguay deployment.

## What the demo showcases

- **Localization**: full Spanish translation, and partial Guaraní (Avañe'ẽ) translation with English fallback.
- **Per-locale formatting**: `common.numberLocale: "es-PY"` produces Paraguay's `1.234.567,89` number format regardless of viewer locale.
- **Per-locale `og:locale`**: `es_PY` / `gn_PY` for social previews.
- **Wildfire hazard**: same indicator slug as India (`flood-hazard`) but the `factors.hazard.name` message overrides the display to "Riesgo de incendio forestal" / "Tata mymba apañuãi".
- **Asset pass-through**: logo and hero foreground ship as TS-imported SVGs. The frontend's `<Image>` reads natural dimensions from the asset and emits a single fetch with `sizes` derived from the asset's width.
