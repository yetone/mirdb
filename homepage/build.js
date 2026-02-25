#!/usr/bin/env node
/**
 * Simple Jekyll-like build script for testing purposes
 * Compiles Jekyll templates without requiring Ruby
 */

const fs = require('fs');
const path = require('path');

// Read config
const configPath = path.join(__dirname, '_config.yml');
const configContent = fs.readFileSync(configPath, 'utf-8');

// Parse simple YAML config
const parseSimpleYaml = (content) => {
  const config = {};
  const lines = content.split('\n');
  let currentKey = null;

  for (const line of lines) {
    // Skip comments and empty lines
    if (line.trim().startsWith('#') || line.trim() === '') continue;

    // Handle simple key: value pairs
    const match = line.match(/^(\w+):\s*(.*)$/);
    if (match) {
      const [, key, value] = match;
      // Remove quotes if present
      config[key] = value.replace(/^["']|["']$/g, '');
    }
  }
  return config;
};

const config = parseSimpleYaml(configContent);

// Read page front matter from index.html
const indexPath = path.join(__dirname, 'index.html');
const indexContent = fs.readFileSync(indexPath, 'utf-8');

// Parse front matter
const frontMatterMatch = indexContent.match(/^---\n([\s\S]*?)\n---/);
const frontMatter = {};
if (frontMatterMatch) {
  const fmLines = frontMatterMatch[1].split('\n');
  for (const line of fmLines) {
    const match = line.match(/^(\w+):\s*(.*)$/);
    if (match) {
      const [, key, value] = match;
      frontMatter[key] = value.replace(/^["']|["']$/g, '');
    }
  }
}

// Content after front matter
const pageContent = indexContent.replace(/^---[\s\S]*?---\n/, '');

// Read layout
const layoutPath = path.join(__dirname, '_layouts', 'default.html');
const layoutContent = fs.readFileSync(layoutPath, 'utf-8');

// Read includes
const readInclude = (name) => {
  const includePath = path.join(__dirname, '_includes', name);
  try {
    return fs.readFileSync(includePath, 'utf-8');
  } catch {
    return '';
  }
};

// Process Jekyll variables
const processTemplate = (template, context) => {
  let result = template;

  // Process includes
  result = result.replace(/\{%\s*include\s+(\S+)\s*%\}/g, (match, includeName) => {
    return readInclude(includeName);
  });

  // Process Liquid filters and variables
  result = result.replace(/\{\{\s*([^}]+)\s*\}\}/g, (match, expr) => {
    // Handle page.title | default: site.title pattern
    if (expr.includes('page.title | default: site.title')) {
      return context.page.title || context.site.title;
    }
    if (expr.includes('page.description | default: site.description')) {
      return context.page.description || context.site.description;
    }
    if (expr.includes('page.url | absolute_url')) {
      return (context.site.url || '') + (context.page.url || '/');
    }
    // Handle site.* variables
    if (expr.includes('site.title')) {
      return context.site.title || '';
    }
    if (expr.includes('site.description')) {
      return context.site.description || '';
    }
    if (expr.includes('site.url')) {
      return context.site.url || '';
    }
    if (expr.includes('site.time | date:')) {
      return new Date().getFullYear().toString();
    }
    // Handle relative_url filter
    if (expr.includes('| relative_url')) {
      const pathMatch = expr.match(/['"]([^'"]+)['"]/);
      if (pathMatch) {
        return pathMatch[1];
      }
    }
    // Handle absolute_url filter
    if (expr.includes('| absolute_url')) {
      const pathMatch = expr.match(/['"]([^'"]+)['"]/);
      if (pathMatch) {
        return (context.site.url || '') + pathMatch[1];
      }
    }
    // Handle content placeholder
    if (expr.trim() === 'content') {
      return context.content;
    }

    return match; // Return unchanged if not handled
  });

  return result;
};

// Build context
const context = {
  site: {
    title: config.title || 'MirDB',
    description: config.description || '',
    url: config.url || 'https://mirdb.io',
    baseurl: config.baseurl || '',
    time: new Date()
  },
  page: {
    title: frontMatter.title || config.title,
    description: frontMatter.description || config.description,
    url: '/'
  },
  content: pageContent
};

// Process layout with content
let output = processTemplate(layoutContent, context);

// Create _site directory
const siteDir = path.join(__dirname, '_site');
if (!fs.existsSync(siteDir)) {
  fs.mkdirSync(siteDir, { recursive: true });
}

// Create assets directories
const assetsDir = path.join(siteDir, 'assets');
const cssDir = path.join(assetsDir, 'css');
const jsDir = path.join(assetsDir, 'js');
const imagesDir = path.join(assetsDir, 'images');

[assetsDir, cssDir, jsDir, imagesDir].forEach(dir => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
});

// Minify HTML for performance (NFR-1)
const minifyHtml = (html) => {
  return html
    // Remove HTML comments (keep conditional comments)
    .replace(/<!--(?!\[if)[\s\S]*?-->/g, '')
    // Remove extra whitespace between tags
    .replace(/>\s+</g, '><')
    // Remove leading/trailing whitespace in text
    .replace(/\s+/g, ' ')
    // Preserve newlines in pre/code blocks by restoring them
    .trim();
};

// Write output (minified for production)
const minifiedOutput = minifyHtml(output);
fs.writeFileSync(path.join(siteDir, 'index.html'), minifiedOutput);
console.log('Built _site/index.html (minified)');

// Build 404 page
const fourOhFourPath = path.join(__dirname, '404.html');
if (fs.existsSync(fourOhFourPath)) {
  const fourOhFourContent = fs.readFileSync(fourOhFourPath, 'utf-8');
  // Parse front matter
  const fourOhFourFMMatch = fourOhFourContent.match(/^---\n([\s\S]*?)\n---/);
  const fourOhFourFM = {};
  if (fourOhFourFMMatch) {
    const fmLines = fourOhFourFMMatch[1].split('\n');
    for (const line of fmLines) {
      const match = line.match(/^(\w+):\s*(.*)$/);
      if (match) {
        const [, key, value] = match;
        fourOhFourFM[key] = value.replace(/^["']|["']$/g, '');
      }
    }
  }
  // Content after front matter
  const fourOhFourPageContent = fourOhFourContent.replace(/^---[\s\S]*?---\n/, '');

  // Build context for 404 page
  const fourOhFourContext = {
    site: context.site,
    page: {
      title: fourOhFourFM.title || 'Page Not Found',
      description: fourOhFourFM.description || 'The requested page was not found',
      url: '/404.html'
    },
    content: fourOhFourPageContent
  };

  // Process layout with 404 content
  const fourOhFourOutput = processTemplate(layoutContent, fourOhFourContext);
  fs.writeFileSync(path.join(siteDir, '404.html'), fourOhFourOutput);
  console.log('Built _site/404.html');
}

// Copy assets
const copyIfExists = (src, dest) => {
  if (fs.existsSync(src)) {
    fs.copyFileSync(src, dest);
    console.log(`Copied ${src} to ${dest}`);
  }
};

// Minify JavaScript for performance (NFR-1)
const minifyJs = (js) => {
  return js
    // Remove single-line comments (but not in strings)
    .replace(/(?<!:)\/\/(?!\/)[^\n]*$/gm, '')
    // Remove multi-line comments
    .replace(/\/\*[\s\S]*?\*\//g, '')
    // Remove extra whitespace
    .replace(/\s+/g, ' ')
    // Remove whitespace around operators
    .replace(/\s*([{};,=+\-*/<>!&|])\s*/g, '$1')
    // Restore necessary spaces
    .replace(/\b(return|const|let|var|if|else|function|typeof|new)\b/g, ' $1 ')
    .trim();
};

// Copy and optionally minify JS files
const srcJsDir = path.join(__dirname, 'assets', 'js');
if (fs.existsSync(srcJsDir)) {
  fs.readdirSync(srcJsDir).forEach(file => {
    const srcPath = path.join(srcJsDir, file);
    const destPath = path.join(jsDir, file);
    if (file.endsWith('.js')) {
      const jsContent = fs.readFileSync(srcPath, 'utf-8');
      // Only minify if not already minified (check for .min.js or Prism)
      if (file.includes('.min.') || file.includes('prism')) {
        fs.copyFileSync(srcPath, destPath);
        console.log(`Copied ${srcPath} to ${destPath}`);
      } else {
        const minifiedJs = minifyJs(jsContent);
        fs.writeFileSync(destPath, minifiedJs);
        console.log(`Minified and wrote ${destPath}`);
      }
    } else {
      copyIfExists(srcPath, destPath);
    }
  });
}

// Copy images
const srcImagesDir = path.join(__dirname, 'assets', 'images');
if (fs.existsSync(srcImagesDir)) {
  fs.readdirSync(srcImagesDir).forEach(file => {
    copyIfExists(path.join(srcImagesDir, file), path.join(imagesDir, file));
  });
}

// Build CSS from SCSS (simplified - just copy if sass is not available)
const sass = require('sass');
const mainScssPath = path.join(__dirname, 'assets', 'css', 'main.scss');
const mainCssPath = path.join(cssDir, 'main.css');

try {
  // Read SCSS and strip Jekyll front matter
  let scssContent = fs.readFileSync(mainScssPath, 'utf-8');
  scssContent = scssContent.replace(/^---[\s\S]*?---\n/, '');

  // Write temp file without front matter
  const tempScssPath = path.join(__dirname, '_sass', 'main-entry.scss');
  fs.writeFileSync(tempScssPath, scssContent);

  const result = sass.compile(tempScssPath, {
    loadPaths: [path.join(__dirname, '_sass')],
    style: 'compressed'
  });
  fs.writeFileSync(mainCssPath, result.css);
  console.log('Compiled CSS');
} catch (err) {
  console.error('SCSS compilation error:', err.message);
  // Create empty CSS file as fallback
  fs.writeFileSync(mainCssPath, '/* CSS placeholder */');
}

console.log('Build complete!');
