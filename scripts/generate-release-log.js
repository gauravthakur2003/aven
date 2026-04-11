#!/usr/bin/env node
/**
 * generate-release-log.js
 *
 * Generates a markdown release log for the current push to main.
 * Run by GitHub Actions after every merge to main.
 *
 * Usage:
 *   node scripts/generate-release-log.js
 *
 * Env vars (set by GitHub Actions):
 *   GITHUB_SHA        — current commit hash
 *   GITHUB_REF_NAME   — branch name (main)
 *   GITHUB_ACTOR      — person who pushed
 *   GITHUB_REPOSITORY — repo name
 *   RELEASE_VERSION   — optional semver tag (e.g. v1.2.3)
 */

const { execSync } = require('child_process');
const fs           = require('fs');
const path         = require('path');

// ── Helpers ───────────────────────────────────────────────────────────────────

function run(cmd) {
  try {
    return execSync(cmd, { encoding: 'utf8' }).trim();
  } catch {
    return '';
  }
}

function today() {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD
}

function nowUtc() {
  return new Date().toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
}

// ── Gather commit data ────────────────────────────────────────────────────────

// Find previous release log to determine range
const releaseDir = path.join(__dirname, '../release_logs');
const prevLogs   = fs.existsSync(releaseDir)
  ? fs.readdirSync(releaseDir).filter(f => f.endsWith('.md')).sort().reverse()
  : [];

// Get the commit hash from the previous release log (if any)
let sinceHash = '';
if (prevLogs.length > 0) {
  const prevContent = fs.readFileSync(path.join(releaseDir, prevLogs[0]), 'utf8');
  const match = prevContent.match(/Commit.*`([a-f0-9]{7,40})`/);
  if (match) sinceHash = match[1];
}

const range      = sinceHash ? `${sinceHash}..HEAD` : 'HEAD~10..HEAD';
const currentSha = process.env.GITHUB_SHA || run('git rev-parse HEAD');
const shortSha   = currentSha.slice(0, 7);
const actor      = process.env.GITHUB_ACTOR || run('git log -1 --format="%an"');
const repo       = process.env.GITHUB_REPOSITORY || 'gauravthakur2003/aven';
const branch     = process.env.GITHUB_REF_NAME || 'main';
const version    = process.env.RELEASE_VERSION || shortSha;

// Raw commits: hash | subject | author | date
const rawCommits = run(`git log ${range} --format="%H|%s|%an|%ad" --date=short`)
  .split('\n')
  .filter(Boolean)
  .map(line => {
    const [hash, subject, author, date] = line.split('|');
    return { hash: hash?.slice(0, 7), subject, author, date };
  });

// Files changed
const filesChanged = run(`git diff --name-only ${range}`)
  .split('\n')
  .filter(Boolean);

// Stats: insertions / deletions
const diffStat = run(`git diff --stat ${range} | tail -1`);

// Categorise commits by prefix
const categories = {
  'New Features':    [],
  'Bug Fixes':       [],
  'Pipeline / Data': [],
  'Tests':           [],
  'Infrastructure':  [],
  'Docs':            [],
  'Other':           [],
};

for (const c of rawCommits) {
  const s = c.subject?.toLowerCase() ?? '';
  if (s.startsWith('feat') || s.includes('add '))                           categories['New Features'].push(c);
  else if (s.startsWith('fix') || s.includes('fix'))                        categories['Bug Fixes'].push(c);
  else if (s.includes('pipeline') || s.includes('scraper') || s.includes('normaliser') || s.includes('llm')) categories['Pipeline / Data'].push(c);
  else if (s.startsWith('test') || s.includes('test'))                      categories['Tests'].push(c);
  else if (s.startsWith('chore') || s.includes('deploy') || s.includes('railway') || s.includes('ci')) categories['Infrastructure'].push(c);
  else if (s.startsWith('docs') || s.includes('readme') || s.includes('release')) categories['Docs'].push(c);
  else                                                                        categories['Other'].push(c);
}

// Packages touched
const packagesTouched = [...new Set(
  filesChanged.map(f => {
    if (f.startsWith('packages/dashboard'))  return '`dashboard`';
    if (f.startsWith('packages/normaliser')) return '`normaliser`';
    if (f.startsWith('packages/scraper'))    return '`scraper`';
    if (f.startsWith('db/'))                 return '`db/migrations`';
    if (f.startsWith('.github/'))            return '`CI/CD`';
    if (f.startsWith('scripts/'))            return '`scripts`';
    return null;
  }).filter(Boolean)
)].join(', ');

// ── Build markdown ─────────────────────────────────────────────────────────────

const ghBase = `https://github.com/${repo}`;

let md = '';

md += `# Release — ${today()}\n\n`;
md += `**Version:** \`${version}\`  \n`;
md += `**Branch:** \`${branch}\`  \n`;
md += `**Commit:** [\`${shortSha}\`](${ghBase}/commit/${currentSha})  \n`;
md += `**Author:** ${actor}  \n`;
md += `**Released:** ${nowUtc()}  \n`;
md += `**Packages touched:** ${packagesTouched || 'various'}  \n`;
md += '\n---\n\n';

// Summary stats
if (diffStat) {
  md += `## Summary\n\n`;
  md += `\`\`\`\n${diffStat}\n\`\`\`\n\n`;
  md += `**${rawCommits.length}** commit${rawCommits.length !== 1 ? 's' : ''} · **${filesChanged.length}** file${filesChanged.length !== 1 ? 's' : ''} changed\n\n`;
}

// Categorised changes
md += `## Changes\n\n`;
for (const [category, commits] of Object.entries(categories)) {
  if (commits.length === 0) continue;
  md += `### ${category}\n\n`;
  for (const c of commits) {
    const link = `[${c.hash}](${ghBase}/commit/${c.hash})`;
    md += `- ${link} — ${c.subject} *(${c.author}, ${c.date})*\n`;
  }
  md += '\n';
}

// Files changed
if (filesChanged.length > 0) {
  md += `## Files Changed\n\n`;

  // Group by package
  const byPackage = {};
  for (const f of filesChanged) {
    const pkg = f.split('/').slice(0, 2).join('/');
    if (!byPackage[pkg]) byPackage[pkg] = [];
    byPackage[pkg].push(f);
  }

  for (const [pkg, files] of Object.entries(byPackage)) {
    md += `**${pkg}/**\n`;
    for (const f of files) {
      md += `- \`${f}\`\n`;
    }
    md += '\n';
  }
}

// All commits (raw log)
if (rawCommits.length > 0) {
  md += `## Full Commit Log\n\n`;
  md += `| Hash | Message | Author | Date |\n`;
  md += `|------|---------|--------|------|\n`;
  for (const c of rawCommits) {
    const link = `[\`${c.hash}\`](${ghBase}/commit/${c.hash})`;
    md += `| ${link} | ${c.subject} | ${c.author} | ${c.date} |\n`;
  }
  md += '\n';
}

md += `---\n*Generated automatically by Aven release bot*\n`;

// ── Write file ─────────────────────────────────────────────────────────────────

if (!fs.existsSync(releaseDir)) fs.mkdirSync(releaseDir, { recursive: true });

const filename = `${today()}-${shortSha}.md`;
const filepath = path.join(releaseDir, filename);
fs.writeFileSync(filepath, md, 'utf8');

console.log(`✅ Release log written: release_logs/${filename}`);
console.log(`   ${rawCommits.length} commits · ${filesChanged.length} files changed`);
