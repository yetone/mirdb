// Jest setup file
const fs = require('fs');
const path = require('path');

// Load HTML content into document
const htmlPath = path.join(__dirname, '..', 'index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');
document.documentElement.innerHTML = html;
