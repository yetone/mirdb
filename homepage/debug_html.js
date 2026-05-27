const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

const HOMEPAGE_DIR = '/workspace/homepage';
const PARTIALS_DIR = path.join(HOMEPAGE_DIR, 'templates/partials');

function preprocessTemplate(html) {
  return html
    .replace(/\{\{\s*get_url\(path=['"]([^'"]+)['"]\)\s*\}\}/g, (match, p1) => '/' + p1)
    .replace(/\{%\s*include\s+["']([^"']+)["']\s*%\}/g, '')
    .replace(/\{%\s*block\s+\w+\s*%\}/g, '')
    .replace(/\{%\s*endblock\s*%\}/g, '');
}

const basePath = path.join(HOMEPAGE_DIR, 'templates/base.html');
const indexPath = path.join(HOMEPAGE_DIR, 'templates/index.html');
let html = fs.readFileSync(basePath, 'utf-8');
const indexHtml = fs.readFileSync(indexPath, 'utf-8');

const blockRegex = /\{%\s*block\s+(\w+)\s*%\}([\s\S]*?)\{%\s*endblock(?:\s+\w+)?\s*%\}/g;
let blockMatch;
while ((blockMatch = blockRegex.exec(indexHtml)) !== null) {
  const blockName = blockMatch[1];
  const blockContent = blockMatch[2];
  console.log('Replacing block:', blockName, 'with content length:', blockContent.length);
  const baseBlockPattern = new RegExp('\\{%\\s*block\\s+' + blockName + '\\s*%\\}([\\s\\S]*?)\\{%\\s*endblock(?:\\s+\\w+)?\\s*%\\}');
  html = html.replace(baseBlockPattern, blockContent);
}

console.log('After block replace, includes remaining:');
const includes = [...html.matchAll(/\{%\s*include\s+["']([^"']+)["']\s*%\}/g)];
includes.forEach(m => console.log('  ', m[1]));

let safety = 0;
while (safety++ < 20) {
  const matches = [...html.matchAll(/\{%\s*include\s+["']([^"']+)["']\s*%\}/g)];
  if (matches.length === 0) break;
  for (const m of matches) {
    const includePath = m[1];
    const partialFile = path.basename(includePath).replace('.html', '') + '.html';
    const partialPath = path.join(PARTIALS_DIR, partialFile);
    let partialContent = '';
    if (fs.existsSync(partialPath)) {
      partialContent = fs.readFileSync(partialPath, 'utf-8');
    }
    html = html.replace(m[0], partialContent);
  }
}

html = html.replace(/\{\{\s*get_url\(path=['"]([^'"]+)['"]\)\s*\}\}/g, (m, p1) => '/' + p1);

const dom = new JSDOM(preprocessTemplate(html));
const doc = dom.window.document;

console.log('\nParsed results:');
console.log('Articles:', doc.querySelectorAll('article').length);
console.log('Sections:', doc.querySelectorAll('section').length);
console.log('Headers:', doc.querySelectorAll('header').length);
console.log('Navs:', doc.querySelectorAll('nav').length);
console.log('Footers:', doc.querySelectorAll('footer').length);
console.log('Main:', doc.querySelectorAll('main').length);
