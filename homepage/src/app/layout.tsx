import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'MirDB - Persistent Key-Value Store with Memcached Compatibility',
  description: 'MirDB is a persistent key-value store written in Rust that implements the Memcached protocol. Built with LSM tree architecture for durability and performance.',
  keywords: ['key-value store', 'memcached', 'rust', 'lsm tree', 'database', 'persistent storage'],
  openGraph: {
    title: 'MirDB - Persistent Key-Value Store',
    description: 'A persistent key-value store with Memcached protocol compatibility, built in Rust.',
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
      <body className="min-h-screen bg-white dark:bg-slate-900 text-slate-900 dark:text-slate-50">
        {children}
      </body>
    </html>
  );
}
