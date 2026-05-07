import type { Metadata, Viewport } from 'next';
import './globals.css';
import { ThemeProvider } from '@/hooks/useTheme';
import ThemeToggle from '@/components/ThemeToggle';

const themeScript = `
  (function() {
    try {
      var saved = localStorage.getItem('mirdb-theme');
      var theme = saved || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
      document.documentElement.classList.toggle('dark', theme === 'dark');
    } catch(e) {}
  })();
`;

const SITE_URL = 'https://yetone.github.io/mirdb';
const REPOSITORY_URL = 'https://github.com/yetone/mirdb';
const SITE_TITLE = 'MirDB - Persistent Key-Value Store with Memcached Protocol';
const SITE_DESCRIPTION =
  'MirDB is a persistent key-value store written in Rust with memcached protocol compatibility and LSM-tree architecture.';
const SITE_IMAGE = `${SITE_URL}/logo.gif`;

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
};

export const metadata: Metadata = {
  metadataBase: new URL(SITE_URL),
  title: SITE_TITLE,
  description: SITE_DESCRIPTION,
  applicationName: 'MirDB',
  keywords: [
    'MirDB',
    'key-value store',
    'memcached',
    'LSM-tree',
    'Rust',
    'database',
    'persistent storage',
    'SSTable',
  ],
  authors: [{ name: 'MirDB contributors', url: REPOSITORY_URL }],
  creator: 'MirDB contributors',
  publisher: 'MirDB',
  alternates: {
    canonical: SITE_URL,
  },
  openGraph: {
    title: SITE_TITLE,
    description: SITE_DESCRIPTION,
    url: SITE_URL,
    siteName: 'MirDB',
    type: 'website',
    locale: 'en_US',
    images: [
      {
        url: SITE_IMAGE,
        width: 1200,
        height: 630,
        alt: 'MirDB logo',
      },
    ],
  },
  twitter: {
    card: 'summary_large_image',
    title: SITE_TITLE,
    description: SITE_DESCRIPTION,
    images: [SITE_IMAGE],
  },
  robots: {
    index: true,
    follow: true,
  },
};

const structuredData = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: 'MirDB',
  description: SITE_DESCRIPTION,
  url: SITE_URL,
  applicationCategory: 'DatabaseApplication',
  operatingSystem: 'Linux, macOS, Windows',
  programmingLanguage: 'Rust',
  codeRepository: REPOSITORY_URL,
  license: 'https://opensource.org/licenses/MIT',
  offers: {
    '@type': 'Offer',
    price: '0',
    priceCurrency: 'USD',
  },
  author: {
    '@type': 'Organization',
    name: 'MirDB contributors',
    url: REPOSITORY_URL,
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData) }}
        />
      </head>
      <body className="min-h-screen antialiased">
        {/* eslint-disable-next-line @next/next/no-sync-scripts */}
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
        <ThemeProvider>
          {children}
          <ThemeToggle />
        </ThemeProvider>
      </body>
    </html>
  );
}
