#!/usr/bin/env node
/*
 * extract_media.js — print top-level @media blocks of a CSS file
 *
 * Usage:
 *   node extract_media.js path/to/file.css
 */

const fs = require('fs');
const path = require('path');

function extractMediaBlocks(css) {
  const blocks = [];
  for (let i = 0; i < css.length; i++) {
    if (css.startsWith('@media', i)) {
      const braceStart = css.indexOf('{', i);
      if (braceStart === -1) break;
      const query = css.slice(i + 6, braceStart).trim();
      let depth = 1;
      let j = braceStart + 1;
      while (j < css.length && depth > 0) {
        if (css[j] === '{') depth++;
        else if (css[j] === '}') depth--;
        if (depth === 0) break;
        j++;
      }
      blocks.push({ query, body: css.slice(braceStart + 1, j) });
      i = j;
    }
  }
  return blocks;
}

function main() {
  const file = process.argv[2];
  if (!file) {
    console.error('Usage: node extract_media.js <css-file>');
    process.exit(1);
  }
  const css = fs.readFileSync(path.resolve(file), 'utf8');
  const blocks = extractMediaBlocks(css);
  console.log(`Found ${blocks.length} @media block(s)\n`);
  blocks.forEach((b, idx) => {
    console.log(`#${idx + 1}  @media ${b.query}`);
    console.log(b.body.trim().split('\n').map(l => '    ' + l).join('\n'));
    console.log();
  });
}

if (require.main === module) main();
module.exports = { extractMediaBlocks };
