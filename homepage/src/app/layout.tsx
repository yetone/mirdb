import type { Metadata } from 'next';
import './globals.css';

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
    <html lang="en">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  );
}
