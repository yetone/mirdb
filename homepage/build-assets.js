/**
 * Build script for asset optimization
 * - Converts images to WebP format
 * - Minifies CSS
 */

const sharp = require('sharp');
const fs = require('fs');
const path = require('path');

const assetsDir = path.join(__dirname, '..', 'assets');
const homepageDir = __dirname;

async function convertToWebP(inputPath, outputPath) {
  try {
    // Check if input is a GIF (animated)
    const ext = path.extname(inputPath).toLowerCase();

    if (ext === '.gif') {
      // For GIFs, we'll create an optimized static WebP from first frame
      // and keep original GIF for animation
      await sharp(inputPath, { animated: false })
        .webp({ quality: 80 })
        .toFile(outputPath);
      console.log(`Converted ${inputPath} to ${outputPath} (first frame)`);
    } else {
      await sharp(inputPath)
        .webp({ quality: 85 })
        .toFile(outputPath);
      console.log(`Converted ${inputPath} to ${outputPath}`);
    }
    return true;
  } catch (err) {
    console.error(`Error converting ${inputPath}:`, err.message);
    return false;
  }
}

function minifyCSS(css) {
  return css
    // Remove comments
    .replace(/\/\*[\s\S]*?\*\//g, '')
    // Remove whitespace around special characters
    .replace(/\s*([{}:;,>~+])\s*/g, '$1')
    // Remove multiple spaces
    .replace(/\s+/g, ' ')
    // Remove spaces at start/end
    .trim()
    // Remove unnecessary semicolons before closing braces
    .replace(/;}/g, '}')
    // Remove whitespace after opening brace
    .replace(/{\s+/g, '{')
    // Remove whitespace before closing brace
    .replace(/\s+}/g, '}');
}

async function main() {
  console.log('Starting asset optimization...\n');

  // 1. Convert images to WebP
  console.log('=== Converting images to WebP ===');
  const imageFiles = fs.readdirSync(assetsDir).filter(f =>
    ['.gif', '.png', '.jpg', '.jpeg'].includes(path.extname(f).toLowerCase())
  );

  for (const file of imageFiles) {
    const inputPath = path.join(assetsDir, file);
    const baseName = path.basename(file, path.extname(file));
    const outputPath = path.join(assetsDir, `${baseName}.webp`);

    // Skip if WebP already exists and is newer than source
    if (fs.existsSync(outputPath)) {
      const srcStat = fs.statSync(inputPath);
      const dstStat = fs.statSync(outputPath);
      if (dstStat.mtime > srcStat.mtime) {
        console.log(`Skipping ${file} (WebP is up to date)`);
        continue;
      }
    }

    await convertToWebP(inputPath, outputPath);
  }

  // 2. Minify CSS
  console.log('\n=== Minifying CSS ===');
  const cssPath = path.join(homepageDir, 'styles.css');
  const minCssPath = path.join(homepageDir, 'styles.min.css');

  if (fs.existsSync(cssPath)) {
    const css = fs.readFileSync(cssPath, 'utf-8');
    const minified = minifyCSS(css);

    fs.writeFileSync(minCssPath, minified);

    const originalSize = css.length;
    const minifiedSize = minified.length;
    const savings = ((1 - minifiedSize / originalSize) * 100).toFixed(1);

    console.log(`Original CSS: ${originalSize} bytes`);
    console.log(`Minified CSS: ${minifiedSize} bytes`);
    console.log(`Savings: ${savings}%`);
    console.log(`Written to: ${minCssPath}`);
  }

  console.log('\n=== Asset optimization complete ===');
}

main().catch(console.error);
