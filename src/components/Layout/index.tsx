import React, { type ReactNode } from 'react';
import { ThemeProvider } from '../ThemeToggle';
import ThemeToggle from '../ThemeToggle';

function Header() {
  return (
    <header className="fixed top-0 left-0 right-0 z-40 bg-white/80 dark:bg-gray-950/80 backdrop-blur-sm border-b border-gray-200 dark:border-gray-800">
      <nav className="max-w-6xl mx-auto px-4 py-3 flex items-center justify-between" aria-label="Site navigation">
        <span className="font-semibold text-brand-600 dark:text-brand-400">MirDB</span>
        <ThemeToggle />
      </nav>
    </header>
  );
}

function Footer() {
  return (
    <footer className="py-8 text-center text-sm text-gray-500 dark:text-gray-400 border-t border-gray-200 dark:border-gray-800">
      <p>MirDB — A Persistent Key-Value Store with Memcached Protocol</p>
    </footer>
  );
}

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <ThemeProvider>
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-brand-500 focus:text-white focus:rounded focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2"
      >
        Skip to main content
      </a>
      <Header />
      <main id="main-content">{children}</main>
      <Footer />
    </ThemeProvider>
  );
}
