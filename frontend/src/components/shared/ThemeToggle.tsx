/**
 * Theme toggle UI element.
 * Owner: Scenario 6 - Theme System Compatibility
 *
 * Displays a dropdown of all supported themes and updates ThemeContext
 * when a selection is made.
 */

import React, { useState, useRef, useEffect } from 'react';
import { useTheme } from '../../contexts/ThemeContext';
import { SUPPORTED_THEMES } from '../../utils/constants';

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [isOpen]);

  return (
    <div ref={containerRef} data-testid="theme-toggle">
      <button
        data-testid="theme-toggle-button"
        onClick={() => setIsOpen(!isOpen)}
        aria-label="Toggle theme menu"
        aria-expanded={isOpen}
        aria-haspopup="listbox"
      >
        Theme: {theme}
      </button>
      {isOpen && (
        <ul role="listbox" aria-label="Select theme" data-testid="theme-menu">
          {SUPPORTED_THEMES.map((t) => (
            <li key={t} role="option" aria-selected={t === theme}>
              <button
                onClick={() => {
                  setTheme(t);
                  setIsOpen(false);
                }}
                data-testid={`theme-option-${t}`}
              >
                {t}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
