/**
 * Main Application Component
 * Owner: Scenario 1 - Homepage Layout & Structure
 *
 * Renders all sections in order.
 * Note: This is a basic setup by scenario 2 (Navbar).
 * Scenario 1 will finalize the layout structure.
 */

import { useState, useEffect } from 'react';
import { Navbar } from './components/layout/Navbar';

function App() {
  const [isDarkMode, setIsDarkMode] = useState(false);

  useEffect(() => {
    // Check for saved theme preference or system preference
    const savedTheme = localStorage.getItem('theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme === 'dark' || (!savedTheme && prefersDark)) {
      setIsDarkMode(true);
      document.documentElement.classList.add('dark');
    }
  }, []);

  const toggleTheme = () => {
    setIsDarkMode(!isDarkMode);
    if (isDarkMode) {
      document.documentElement.classList.remove('dark');
      localStorage.setItem('theme', 'light');
    } else {
      document.documentElement.classList.add('dark');
      localStorage.setItem('theme', 'dark');
    }
  };

  return (
    <div className="min-h-screen bg-white dark:bg-gray-900">
      <Navbar onThemeToggle={toggleTheme} isDarkMode={isDarkMode} />

      {/* Placeholder content - other scenarios will add their sections */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="text-center">
          <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-4">
            MirDB
          </h1>
          <p className="text-xl text-gray-600 dark:text-gray-300">
            A Persistent Key-Value Store with Memcached Protocol
          </p>
        </div>
      </main>
    </div>
  );
}

export default App;
