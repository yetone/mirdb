/**
 * Application entry point for MirDB Homepage.
 */

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import './styles/globals.css';

// Apply theme based on stored preference or default to dark mode
const storedTheme = localStorage.getItem('mirdb-theme');
if (storedTheme === 'light') {
  document.documentElement.classList.remove('dark');
} else {
  // Default to dark mode if no preference or preference is 'dark'
  document.documentElement.classList.add('dark');
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
