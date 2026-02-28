/**
 * Image optimization script for MirDB Homepage
 * Creates optimized versions of images in src/images/
 * Targets: < 500KB per image for performance requirements
 */

const sharp = require('sharp');
const fs = require('fs');
const path = require('path');

const SRC_DIR = path.join(__dirname, '..', 'src', 'images');
const BACKUP_DIR = path.join(__dirname, '..', 'src', 'images', 'originals');

async function optimizeImages() {
  // Create backup directory
  if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
  }

  const files = fs.readdirSync(SRC_DIR);

  for (const file of files) {
    const srcPath = path.join(SRC_DIR, file);
    const stats = fs.statSync(srcPath);

    // Skip directories and already small files
    if (stats.isDirectory()) continue;
    if (stats.size <= 500 * 1024) {
      console.log(`${file}: ${(stats.size / 1024).toFixed(2)} KB - OK (under 500KB)`);
      continue;
    }

    console.log(`${file}: ${(stats.size / 1024).toFixed(2)} KB - Optimizing...`);

    // Backup original
    const backupPath = path.join(BACKUP_DIR, file);
    if (!fs.existsSync(backupPath)) {
      fs.copyFileSync(srcPath, backupPath);
      console.log(`  -> Backed up original to ${backupPath}`);
    }

    if (file.endsWith('.gif')) {
      try {
        // Read GIF info to determine if animated
        const metadata = await sharp(srcPath).metadata();

        // For logo.gif - create optimized static PNG (used in hero section)
        if (file === 'logo.gif') {
          // Extract first frame and create optimized PNG
          await sharp(srcPath, { pages: 1 })
            .resize(120, 120, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
            .png({ compressionLevel: 9, quality: 85 })
            .toFile(srcPath.replace('.gif', '.png'));

          // Also create a small optimized GIF
          await sharp(srcPath, { pages: 1 })
            .resize(120, 120, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
            .gif()
            .toFile(srcPath + '.optimized');

          fs.renameSync(srcPath + '.optimized', srcPath);

          const newStats = fs.statSync(srcPath);
          console.log(`  -> Optimized to ${(newStats.size / 1024).toFixed(2)} KB`);
        }
        // For usage.gif - this might be an animated demo, create optimized version
        else if (file === 'usage.gif') {
          // Create optimized WebP which supports animation
          await sharp(srcPath, { pages: 1 })
            .resize(800, 600, { fit: 'inside', withoutEnlargement: true })
            .gif()
            .toFile(srcPath + '.optimized');

          fs.renameSync(srcPath + '.optimized', srcPath);

          const newStats = fs.statSync(srcPath);
          console.log(`  -> Optimized to ${(newStats.size / 1024).toFixed(2)} KB`);
        }
      } catch (error) {
        console.log(`  -> Error optimizing: ${error.message}`);
      }
    }
  }

  console.log('\nOptimization complete!');

  // Show final sizes
  console.log('\nFinal image sizes:');
  const finalFiles = fs.readdirSync(SRC_DIR);
  for (const file of finalFiles) {
    const filePath = path.join(SRC_DIR, file);
    const stats = fs.statSync(filePath);
    if (!stats.isDirectory()) {
      const status = stats.size > 500 * 1024 ? 'EXCEEDS LIMIT' : 'OK';
      console.log(`  ${file}: ${(stats.size / 1024).toFixed(2)} KB - ${status}`);
    }
  }
}

optimizeImages().catch(console.error);
