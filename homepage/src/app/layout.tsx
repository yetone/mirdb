import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';

const inter = Inter({ subsets: ['latin'] });

const SITE_URL = 'https://mirdb.io';

export const metadata: Metadata = {
  title: 'MirDB - Persistent Key-Value Store',
  description: 'MirDB is a persistent key-value store written in Rust with Memcached protocol compatibility. Fast, reliable data persistence using LSM tree architecture.',
  keywords: ['mirdb', 'key-value store', 'memcached', 'rust', 'lsm tree', 'persistent storage'],
  authors: [{ name: 'MirDB Team' }],
  metadataBase: new URL(SITE_URL),
  alternates: {
    canonical: '/',
  },
  openGraph: {
    title: 'MirDB - Persistent Key-Value Store',
    description: 'Drop-in replacement for memcached with data persistence using LSM tree architecture',
    type: 'website',
    url: SITE_URL,
    siteName: 'MirDB',
    images: [
      {
        url: '/images/og-image.png',
        width: 1200,
        height: 630,
        alt: 'MirDB - Persistent Key-Value Store',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'MirDB - Persistent Key-Value Store',
    description: 'Drop-in replacement for memcached with data persistence',
    images: ['/images/og-image.png'],
  },
};

const jsonLd = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: 'MirDB',
  applicationCategory: 'DeveloperApplication',
  operatingSystem: 'Linux, macOS, Windows',
  description: 'A persistent key-value store written in Rust that implements the Memcached protocol',
  url: SITE_URL,
  author: {
    '@type': 'Organization',
    name: 'MirDB Team',
  },
  license: 'https://opensource.org/licenses/MIT',
  programmingLanguage: 'Rust',
  offers: {
    '@type': 'Offer',
    price: '0',
    priceCurrency: 'USD',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
        />
      </head>
      <body className={inter.className}>{children}</body>
    </html>
  );
}
