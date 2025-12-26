// Jest setup file
const fs = require('fs');
const path = require('path');

// Load HTML content into document
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

// Extract lang attribute from the HTML string
const langMatch = html.match(/<html[^>]*\slang=["']([^"']+)["']/i);
if (langMatch && langMatch[1]) {
  document.documentElement.setAttribute('lang', langMatch[1]);
}

// Set document content (using the full innerHTML approach)
document.documentElement.innerHTML = html;
