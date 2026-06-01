# IDS-DRR: Paraguay branding

Demo frontend branding package for the Paraguay deployment. Drops in
where the default `branding-stub` would go.

## Layout

```
ids-drr-paraguay-branding/
  package.json                     declares ids-drr-branding peer deps
  src/
    index.ts                       the DeploymentConfig surface
    messages/
      es.json                      Spanish (full)
      gn.json                      Guaraní (partial — falls back to English)
  assets/
    logo.svg                       Header logo (small wordmark)
    heroForeground.svg             Hero foreground (large wordmark)
    paraguay-icon.svg              State-card icon
```

## Wiring it up with Docker Compose

In the repo root (or via your `.env`):

```bash
BRANDING_PACKAGE=ids-drr-paraguay-branding
```

Then `docker compose up -d frontend` mounts this package at
`/app/branding-stub` inside the frontend container, so the npm symlink
`node_modules/ids-drr-branding → ../branding-stub` resolves to this
deployment's branding without rebuilding the image.

## What the demo showcases

- **Localization**: full Spanish translation; partial Guaraní (Avañe'ẽ)
  with English fallback for unsubmitted strings.
- **Per-locale formatting**: `common.numberLocale: "es-PY"` produces
  Paraguay's `1.234.567,89` number format regardless of viewer locale.
- **Per-locale `og:locale`**: `es_PY` / `gn_PY` for social previews.
- **Wildfire hazard**: same indicator slug as India (`flood-hazard`)
  but the `factors.hazard.name` message overrides the display to
  "Riesgo de incendio forestal" / "Tata mymba apañuãi".
- **Asset pass-through**: logo and hero foreground ship as TS-imported
  SVGs; the frontend's `<Image>` reads natural dimensions from the
  asset and emits a single fetch with `sizes` derived from the asset's
  width.

## Replacing the placeholder artwork

The wordmarks (`logo.svg`, `heroForeground.svg`) are placeholder text
in Paraguay flag colours. Drop in your own SVG/PNG assets and update
the imports in `src/index.ts` to match.
