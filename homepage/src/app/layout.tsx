import type { Metadata } from 'next';
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

export const metadata: Metadata = {
  title: 'MirDB - A Persistent Key-Value Store',
  description:
    'MirDB is a persistent key-value store written in Rust with memcached protocol compatibility and LSM-tree architecture.',
  openGraph: {
    title: 'MirDB - A Persistent Key-Value Store',
    description:
      'MirDB is a persistent key-value store written in Rust with memcached protocol compatibility.',
    type: 'website',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
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
