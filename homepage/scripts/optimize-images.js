/**
 * Image optimization script for MirDB Homepage
 * Optimizes GIF images to meet performance requirements (< 500KB each)
 */

const sharp = require('sharp');
const fs = require('fs');
const path = require('path');

const SRC_DIR = path.join(__dirname, '..', 'src', 'images');
const OPTIMIZED_DIR = path.join(__dirname, '..', 'dist', 'images');

// Ensure dist/images directory exists
if (!fs.existsSync(OPTIMIZED_DIR)) {
  fs.mkdirSync(OPTIMIZED_DIR, { recursive: true });
}

async function optimizeImages() {
  const files = fs.readdirSync(SRC_DIR);

  for (const file of files) {
    const srcPath = path.join(SRC_DIR, file);
    const destPath = path.join(OPTIMIZED_DIR, file);
    const stats = fs.statSync(srcPath);

    console.log(`Processing ${file} (${(stats.size / 1024).toFixed(2)} KB)...`);

    if (file.endsWith('.gif')) {
      // For GIF files, check if they're animated
      // If too large, convert to static image or reduce quality
      if (stats.size > 500 * 1024) {
        // Convert first frame to PNG for static display
        const baseName = path.basename(file, '.gif');
        const pngPath = path.join(OPTIMIZED_DIR, `${baseName}.png`);

        try {
          await sharp(srcPath, { pages: 1 })
            .resize(400, 400, { fit: 'inside', withoutEnlargement: true })
            .png({ quality: 80 })
            .toFile(pngPath);

          const newStats = fs.statSync(pngPath);
          console.log(`  -> Converted to PNG: ${(newStats.size / 1024).toFixed(2)} KB`);
        } catch (error) {
          // If conversion fails, copy the original
          console.log(`  -> Could not convert, copying original`);
          fs.copyFileSync(srcPath, destPath);
        }
      } else {
        // Copy small GIFs as-is
        fs.copyFileSync(srcPath, destPath);
        console.log(`  -> Copied as-is (under 500KB)`);
      }
    } else if (file.endsWith('.ico')) {
      // Copy favicon as-is
      fs.copyFileSync(srcPath, destPath);
      console.log(`  -> Copied favicon`);
    } else {
      // Copy other files as-is
      fs.copyFileSync(srcPath, destPath);
      console.log(`  -> Copied as-is`);
    }
  }

  console.log('\nImage optimization complete!');
}

optimizeImages().catch(console.error);
