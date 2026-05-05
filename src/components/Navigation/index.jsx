import React, { useState } from 'react';
import { Button } from '../shared/Button';

export const Navigation = () => {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const navigationItems = [
    { name: 'About', href: '#about' },
    { name: 'Features', href: '#features' },
    { name: 'Getting Started', href: '#getting-started' },
    { name: 'Documentation', href: '#documentation' },
  ];

  return (
    <header className="bg-white shadow-md">
      <div className="container mx-auto px-4">
        <div className="flex justify-between items-center py-4">
          <div className="text-xl font-bold">MirDB</div>

          {/* Desktop Navigation */}
          <nav className="hidden md:flex space-x-8">
            {navigationItems.map((item) => (
              <a key={item.name} href={item.href} className="text-gray-700 hover:text-blue-600">
                {item.name}
              </a>
            ))}
            <a href="https://github.com/yetone/mirdb" target="_blank" rel="noopener noreferrer">
              <Button variant="secondary">GitHub</Button>
            </a>
          </nav>

          {/* Mobile Menu Button */}
          <button
            className="md:hidden"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            aria-label="Toggle menu"
          >
            ☰
          </button>
        </div>

        {/* Mobile Navigation */}
        {mobileMenuOpen && (
          <nav className="md:hidden py-2">
            {navigationItems.map((item) => (
              <a
                key={item.name}
                href={item.href}
                className="block py-2 text-gray-700 hover:text-blue-600"
                onClick={() => setMobileMenuOpen(false)}
              >
                {item.name}
              </a>
            ))}
            <a
              href="https://github.com/yetone/mirdb"
              target="_blank"
              rel="noopener noreferrer"
              className="block py-2"
              onClick={() => setMobileMenuOpen(false)}
            >
              <Button variant="secondary">GitHub</Button>
            </a>
          </nav>
        )}
      </div>
    </header>
  );
};