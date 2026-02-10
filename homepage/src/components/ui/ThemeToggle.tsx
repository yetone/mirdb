/**
 * Theme Toggle Component
 * Owner: Scenario 6 - Dark/Light Theme Toggle
 *
 * Toggle button for switching between themes:
 * - Sun/moon icon indicator
 * - Smooth transition animation
 * - Accessible button with aria-label
 */

import React from 'react';
import { useTheme } from '../../hooks/useTheme';

const buttonStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  width: '40px',
  height: '40px',
  padding: 0,
  border: 'none',
  borderRadius: 'var(--radius-md)',
  backgroundColor: 'transparent',
  cursor: 'pointer',
  color: 'var(--text-primary)',
  transition: 'background-color 0.2s ease, color 0.2s ease, transform 0.2s ease',
};

const buttonHoverStyles: React.CSSProperties = {
  backgroundColor: 'var(--bg-secondary)',
};

const iconStyles: React.CSSProperties = {
  width: '24px',
  height: '24px',
  transition: 'transform 0.3s ease, opacity 0.3s ease',
};

/**
 * Sun Icon SVG component
 */
const SunIcon: React.FC = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    style={iconStyles}
    aria-hidden="true"
    data-testid="sun-icon"
  >
    <circle cx="12" cy="12" r="5" />
    <line x1="12" y1="1" x2="12" y2="3" />
    <line x1="12" y1="21" x2="12" y2="23" />
    <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
    <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
    <line x1="1" y1="12" x2="3" y2="12" />
    <line x1="21" y1="12" x2="23" y2="12" />
    <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
    <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
  </svg>
);

/**
 * Moon Icon SVG component
 */
const MoonIcon: React.FC = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    style={iconStyles}
    aria-hidden="true"
    data-testid="moon-icon"
  >
    <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
  </svg>
);

interface ThemeToggleProps {
  className?: string;
}

export const ThemeToggle: React.FC<ThemeToggleProps> = ({ className }) => {
  const { theme, toggleTheme, isDark } = useTheme();
  const [isHovered, setIsHovered] = React.useState(false);

  const ariaLabel = isDark ? 'Switch to light mode' : 'Switch to dark mode';

  const combinedStyles: React.CSSProperties = {
    ...buttonStyles,
    ...(isHovered ? buttonHoverStyles : {}),
  };

  return (
    <button
      type="button"
      onClick={toggleTheme}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      style={combinedStyles}
      className={className}
      aria-label={ariaLabel}
      aria-pressed={isDark}
      data-testid="theme-toggle"
      data-theme={theme}
    >
      {isDark ? <SunIcon /> : <MoonIcon />}
    </button>
  );
};

export default ThemeToggle;
