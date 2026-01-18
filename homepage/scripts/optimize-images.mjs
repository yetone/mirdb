import sharp from 'sharp';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDir = path.join(__dirname, '..', 'public');

async function optimizeImages() {
  const logoPath = path.join(publicDir, 'logo.gif');
  const webpPath = path.join(publicDir, 'logo.webp');
  const pngFallbackPath = path.join(publicDir, 'logo.png');

  console.log('Optimizing images...');

  // Check if original logo exists
  if (!fs.existsSync(logoPath)) {
    console.error('logo.gif not found');
    process.exit(1);
  }

  const originalSize = fs.statSync(logoPath).size;
  console.log(`Original logo.gif size: ${(originalSize / 1024).toFixed(2)} KB`);

  try {
    // Extract first frame from GIF and convert to WebP (static image)
    // Use smaller dimensions while maintaining quality for web
    await sharp(logoPath, { animated: false })
      .resize(192, 192, {
        fit: 'contain',
        background: { r: 0, g: 0, b: 0, alpha: 0 }
      })
      .webp({ quality: 85 })
      .toFile(webpPath);

    const webpSize = fs.statSync(webpPath).size;
    console.log(`WebP size: ${(webpSize / 1024).toFixed(2)} KB`);
    console.log(`Reduction: ${((1 - webpSize / originalSize) * 100).toFixed(1)}%`);

    // Also create a PNG fallback for browsers that don't support WebP
    await sharp(logoPath, { animated: false })
      .resize(192, 192, {
        fit: 'contain',
        background: { r: 0, g: 0, b: 0, alpha: 0 }
      })
      .png({ quality: 85, compressionLevel: 9 })
      .toFile(pngFallbackPath);

    const pngSize = fs.statSync(pngFallbackPath).size;
    console.log(`PNG fallback size: ${(pngSize / 1024).toFixed(2)} KB`);

    console.log('Image optimization complete!');
  } catch (error) {
    console.error('Error optimizing images:', error.message);
    process.exit(1);
  }
}

optimizeImages();
