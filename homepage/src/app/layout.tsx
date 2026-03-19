import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'MirDB - Persistent Key-Value Store with Memcached Compatibility',
  description: 'MirDB is a persistent key-value store written in Rust that implements the Memcached protocol. Drop-in replacement for memcached with data persistence using LSM tree architecture.',
  keywords: ['mirdb', 'key-value store', 'memcached', 'rust', 'lsm tree', 'persistent storage'],
  authors: [{ name: 'MirDB Team' }],
  openGraph: {
    title: 'MirDB - Persistent Key-Value Store',
    description: 'Drop-in replacement for memcached with data persistence',
    type: 'website',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={inter.className}>{children}</body>
    </html>
  );
}
