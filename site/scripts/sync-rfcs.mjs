// Copies docs/rfcs/*.md into the Starlight content tree, deriving frontmatter
// from each RFC's H1 and rewriting links: sibling RFCs become relative page
// links; anything else relative falls back to the file on GitHub.
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const sourceDir = path.resolve(scriptDir, '../../docs/rfcs');
const outputDir = path.resolve(scriptDir, '../src/content/docs/rfcs');
const githubBlobBase = 'https://github.com/alexpropp/moraine/blob/main';

const excluded = new Set(['0000-template.md', 'README.md']);

export function transformRfc(filename, source) {
  const lines = source.split('\n');
  const h1Index = lines.findIndex((line) => line.startsWith('# '));
  if (h1Index === -1) throw new Error(`${filename}: no H1 title`);

  const title = lines[h1Index].slice(2).trim();
  const body = lines
    .slice(h1Index + 1)
    .join('\n')
    .replace(/^\n+/, '');

  const rewritten = body.replace(/\]\(([^)\s]+)\)/g, (match, target) => {
    if (/^(https?:|mailto:|#)/.test(target)) return match;

    const [file, fragment] = target.split('#');
    const anchor = fragment === undefined ? '' : `#${fragment}`;
    const isSibling = /^\d{4}-[a-z0-9-]+\.md$/.test(file) && !excluded.has(file);
    if (isSibling) return `](../${file.slice(0, -3)}/${anchor})`;

    const resolved = path.posix.normalize(path.posix.join('docs/rfcs', file));
    return `](${githubBlobBase}/${resolved}${anchor})`;
  });

  const order = Number(filename.slice(0, 4));
  const label = title.replace(/^RFC /, '');
  const frontmatter = [
    '---',
    `title: ${JSON.stringify(title)}`,
    `sidebar: { label: ${JSON.stringify(label)}, order: ${order} }`,
    '---',
    '',
  ].join('\n');

  return frontmatter + rewritten;
}

export async function syncRfcs() {
  const entries = await readdir(sourceDir);
  const rfcs = entries
    .filter((name) => name.endsWith('.md') && !excluded.has(name))
    .sort();

  await rm(outputDir, { recursive: true, force: true });
  await mkdir(outputDir, { recursive: true });

  for (const name of rfcs) {
    const source = await readFile(path.join(sourceDir, name), 'utf8');
    await writeFile(path.join(outputDir, name), transformRfc(name, source));
  }
  return rfcs.length;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const count = await syncRfcs();
  console.log(`synced ${count} RFCs to src/content/docs/rfcs/`);
}
