// Jest setup file for DOM testing
require('@testing-library/jest-dom');

// Helper to load HTML content into the DOM
global.loadHTML = (htmlContent) => {
  document.body.innerHTML = htmlContent;
};

// Helper to read the index.html file
const fs = require('fs');
const path = require('path');

global.getIndexHTML = () => {
  const htmlPath = path.join(__dirname, '..', 'index.html');
  return fs.readFileSync(htmlPath, 'utf-8');
};

// Helper to extract body content from full HTML
global.extractBodyContent = (html) => {
  const bodyMatch = html.match(/<body[^>]*>([\s\S]*)<\/body>/i);
  return bodyMatch ? bodyMatch[1] : html;
};
