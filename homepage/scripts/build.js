#!/usr/bin/env node

/**
 * Static Site Build Script
 *
 * This script builds the MirDB homepage as a static site by:
 * 1. Cleaning the output directory
 * 2. Copying HTML, CSS, and asset files to dist/
 * 3. Validating the HTML structure
 * 4. Generating a manifest of built files
 */

const fs = require('fs');
const path = require('path');

const SOURCE_DIR = path.resolve(__dirname, '..');
const DIST_DIR = path.resolve(SOURCE_DIR, 'dist');
const ASSETS_DIR = path.resolve(SOURCE_DIR, '..', 'assets');

// Files to include in the build
const FILES_TO_COPY = [
  'index.html',
  '404.html',
  'styles.css'
];

// Asset files to copy
const ASSET_FILES = [
  'logo.gif',
  'usage.gif'
];

/**
 * Clean the dist directory
 */
function cleanDist() {
  if (fs.existsSync(DIST_DIR)) {
    fs.rmSync(DIST_DIR, { recursive: true });
  }
  fs.mkdirSync(DIST_DIR, { recursive: true });
  console.log('✓ Cleaned dist directory');
}

/**
 * Copy a file from source to destination
 */
function copyFile(src, dest) {
  const destDir = path.dirname(dest);
  if (!fs.existsSync(destDir)) {
    fs.mkdirSync(destDir, { recursive: true });
  }
  fs.copyFileSync(src, dest);
}

/**
 * Copy source files to dist
 */
function copySourceFiles() {
  let copiedCount = 0;

  for (const file of FILES_TO_COPY) {
    const srcPath = path.join(SOURCE_DIR, file);
    const destPath = path.join(DIST_DIR, file);

    if (fs.existsSync(srcPath)) {
      copyFile(srcPath, destPath);
      console.log(`  ✓ Copied ${file}`);
      copiedCount++;
    } else {
      console.warn(`  ⚠ File not found: ${file}`);
    }
  }

  console.log(`✓ Copied ${copiedCount} source files`);
  return copiedCount;
}

/**
 * Copy assets directory
 */
function copyAssets() {
  const distAssetsDir = path.join(DIST_DIR, 'assets');
  fs.mkdirSync(distAssetsDir, { recursive: true });

  let copiedCount = 0;

  for (const asset of ASSET_FILES) {
    const srcPath = path.join(ASSETS_DIR, asset);
    const destPath = path.join(distAssetsDir, asset);

    if (fs.existsSync(srcPath)) {
      copyFile(srcPath, destPath);
      console.log(`  ✓ Copied assets/${asset}`);
      copiedCount++;
    } else {
      console.warn(`  ⚠ Asset not found: ${asset}`);
    }
  }

  console.log(`✓ Copied ${copiedCount} asset files`);
  return copiedCount;
}

/**
 * Update asset paths in HTML files to use local assets directory
 */
function updateAssetPaths() {
  const indexPath = path.join(DIST_DIR, 'index.html');

  if (fs.existsSync(indexPath)) {
    let content = fs.readFileSync(indexPath, 'utf8');

    // Update relative path from ../assets/ to ./assets/
    const originalContent = content;
    content = content.replace(/\.\.\/assets\//g, './assets/');

    if (content !== originalContent) {
      fs.writeFileSync(indexPath, content);
      console.log('✓ Updated asset paths in index.html');
    }
  }
}

/**
 * Validate HTML structure
 */
function validateHtml() {
  const indexPath = path.join(DIST_DIR, 'index.html');

  if (!fs.existsSync(indexPath)) {
    throw new Error('index.html not found in dist directory');
  }

  const content = fs.readFileSync(indexPath, 'utf8');

  // Basic HTML validation
  const validations = [
    { check: content.includes('<!DOCTYPE html>'), msg: 'Missing DOCTYPE declaration' },
    { check: content.includes('<html'), msg: 'Missing <html> tag' },
    { check: content.includes('<head>'), msg: 'Missing <head> tag' },
    { check: content.includes('<body>'), msg: 'Missing <body> tag' },
    { check: content.includes('<title>'), msg: 'Missing <title> tag' },
    { check: content.includes('</html>'), msg: 'Missing closing </html> tag' },
    { check: content.includes('lang="en"'), msg: 'Missing lang attribute on html tag' },
    { check: content.includes('charset="UTF-8"'), msg: 'Missing UTF-8 charset declaration' },
    { check: content.includes('viewport'), msg: 'Missing viewport meta tag' }
  ];

  const errors = validations.filter(v => !v.check).map(v => v.msg);

  if (errors.length > 0) {
    throw new Error(`HTML validation failed:\n  - ${errors.join('\n  - ')}`);
  }

  console.log('✓ HTML validation passed');
}

/**
 * Generate build manifest
 */
function generateManifest() {
  const manifest = {
    buildTime: new Date().toISOString(),
    files: []
  };

  function walkDir(dir, basePath = '') {
    const entries = fs.readdirSync(dir, { withFileTypes: true });

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      const relativePath = path.join(basePath, entry.name);

      if (entry.isDirectory()) {
        walkDir(fullPath, relativePath);
      } else {
        const stats = fs.statSync(fullPath);
        manifest.files.push({
          path: relativePath,
          size: stats.size,
          modified: stats.mtime.toISOString()
        });
      }
    }
  }

  walkDir(DIST_DIR);

  const manifestPath = path.join(DIST_DIR, 'manifest.json');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log('✓ Generated build manifest');

  return manifest;
}

/**
 * Main build function
 */
function build() {
  console.log('\n🔨 Building MirDB Homepage...\n');

  try {
    cleanDist();
    console.log('');

    console.log('Copying source files:');
    copySourceFiles();
    console.log('');

    console.log('Copying assets:');
    copyAssets();
    console.log('');

    updateAssetPaths();
    console.log('');

    console.log('Validating build:');
    validateHtml();
    console.log('');

    const manifest = generateManifest();
    console.log('');

    console.log('📦 Build Summary:');
    console.log(`   Files: ${manifest.files.length}`);
    console.log(`   Total size: ${manifest.files.reduce((acc, f) => acc + f.size, 0)} bytes`);
    console.log(`   Output: ${DIST_DIR}`);

    console.log('\n✅ Build completed successfully!\n');
    process.exit(0);
  } catch (error) {
    console.error(`\n❌ Build failed: ${error.message}\n`);
    process.exit(1);
  }
}

// Run build
build();
