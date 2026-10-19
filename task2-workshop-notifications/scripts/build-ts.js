#!/usr/bin/env node
// Hermetic TypeScript Builder (scripts/build-ts.js)
// Compiles src/*.ts -> dist/*.js using @babel/core + @babel/preset-typescript

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');
const SRC_DIR = path.join(ROOT, 'src');
const DIST_DIR = path.join(ROOT, 'dist');

function walk(dir) {
  let results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results = results.concat(walk(fullPath));
    } else if (entry.isFile() && fullPath.endsWith('.ts')) {
      results.push(fullPath);
    }
  }
  return results;
}

function loadBabel() {
  const searchPaths = [
    path.join(ROOT, 'node_modules'),
    '/usr/share/nodejs',
    '/usr/lib/nodejs',
  ];
  for (const base of searchPaths) {
    try {
      const babel = require(require.resolve('@babel/core', { paths: [base] }));
      const presetTs = require.resolve('@babel/preset-typescript', { paths: [base] });
      const pluginCjs = require.resolve('@babel/plugin-transform-modules-commonjs', { paths: [base] });
      return { babel, presetTs, pluginCjs };
    } catch (_) {
      // try next path
    }
  }
  throw new Error('Could not locate TypeScript or Babel TypeScript compiler.');
}

function main() {
  if (fs.existsSync(DIST_DIR)) {
    fs.rmSync(DIST_DIR, { recursive: true, force: true });
  }
  const files = walk(SRC_DIR);
  const { babel, presetTs, pluginCjs } = loadBabel();

  const start = Date.now();
  for (const file of files) {
    const rel = path.relative(SRC_DIR, file);
    const outPath = path.join(DIST_DIR, rel.replace(/\.ts$/, '.js'));
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    const source = fs.readFileSync(file, 'utf8');
    const result = babel.transformSync(source, {
      filename: file,
      presets: [[presetTs, { allowDeclareFields: true }]],
      plugins: [pluginCjs],
      sourceMaps: false,
      comments: true,
    });
    fs.writeFileSync(outPath, result.code, 'utf8');
  }
  console.log(`[build-ts] Compiled ${files.length} TypeScript files to dist/ in ${Date.now() - start}ms.`);
}

main();
