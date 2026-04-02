/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  output: 'export',

  // Image optimization for static export
  images: {
    unoptimized: true,
    // Prefer modern image formats when available
    formats: ['image/webp', 'image/avif'],
  },

  // Performance optimizations
  compress: true,

  // Enable experimental features for better performance
  experimental: {
    // Optimize package imports to reduce bundle size
    optimizePackageImports: ['prismjs'],
  },

  // Production optimizations
  productionBrowserSourceMaps: false,

  // Note: For static export (output: 'export'), headers must be configured
  // at the web server/CDN level (e.g., Netlify, Vercel, nginx).
  // Recommended headers for deployment:
  // - Cache-Control: public, max-age=31536000, immutable (for static assets)
  // - X-Content-Type-Options: nosniff
  // - X-Frame-Options: DENY
  // - X-XSS-Protection: 1; mode=block

  // Webpack optimizations for bundle size
  webpack: (config, { dev, isServer }) => {
    // Production optimizations
    if (!dev && !isServer) {
      // Minimize bundle size
      config.optimization = {
        ...config.optimization,
        minimize: true,
        splitChunks: {
          chunks: 'all',
          minSize: 20000,
          maxSize: 244000,
          cacheGroups: {
            vendor: {
              test: /[\\/]node_modules[\\/]/,
              name: 'vendors',
              chunks: 'all',
            },
          },
        },
      };
    }
    return config;
  },
};

module.exports = nextConfig;
