# site

The project website — [Astro](https://astro.build) +
[Starlight](https://starlight.astro.build), deployed to GitHub Pages at
<https://alexpropp.github.io/moraine/> by `.github/workflows/site.yml`.

Content lives in `src/content/docs/`. The RFCs under the site's Design
section are **not** authored here: `scripts/sync-rfcs.mjs` copies them from
`../docs/rfcs/` before every `dev`/`build` (the synced copies are
gitignored). Edit RFCs at the source.

```sh
npm install
npm test         # sync-script unit tests
npm run dev      # local dev server (syncs RFCs first)
npm run build    # production build to ./dist/
```
