/**
 * Theme store for dark/light mode.
 * Owner: Scenario 5 - Theme Toggle
 *
 * Provides theme state management with:
 * - System preference detection
 * - localStorage persistence
 * - Reactive theme switching
 */

import { writable, type Writable } from 'svelte/store';
import type { Theme } from '$lib/types';

const THEME_STORAGE_KEY = 'mirdb-theme';

/**
 * Check if we're running in a browser environment
 */
function isBrowser(): boolean {
	return typeof window !== 'undefined' && typeof document !== 'undefined';
}

/**
 * Get the system color scheme preference
 */
function getSystemPreference(): Theme {
	if (!isBrowser()) {
		return 'dark';
	}
	return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

/**
 * Get the stored theme preference from localStorage
 */
function getStoredTheme(): Theme | null {
	if (!isBrowser()) {
		return null;
	}
	const stored = localStorage.getItem(THEME_STORAGE_KEY);
	if (stored === 'light' || stored === 'dark') {
		return stored;
	}
	return null;
}

/**
 * Store the theme preference in localStorage
 */
function storeTheme(theme: Theme): void {
	if (!isBrowser()) {
		return;
	}
	localStorage.setItem(THEME_STORAGE_KEY, theme);
}

/**
 * Apply the theme to the document
 */
function applyTheme(theme: Theme): void {
	if (!isBrowser()) {
		return;
	}
	document.documentElement.setAttribute('data-theme', theme);
}

/**
 * Create the initial theme value based on stored preference or system preference
 */
function getInitialTheme(): Theme {
	const stored = getStoredTheme();
	if (stored) {
		return stored;
	}
	return getSystemPreference();
}

/**
 * Theme store - writable store containing the current theme
 */
export const theme: Writable<Theme> = writable<Theme>('dark');

/**
 * Initialize the theme based on stored preference or system preference.
 * Should be called once when the app loads.
 */
export function initTheme(): void {
	const initialTheme = getInitialTheme();
	theme.set(initialTheme);
	applyTheme(initialTheme);
	storeTheme(initialTheme);
}

/**
 * Toggle between light and dark themes
 */
export function toggleTheme(): void {
	theme.update((currentTheme) => {
		const newTheme: Theme = currentTheme === 'light' ? 'dark' : 'light';
		applyTheme(newTheme);
		storeTheme(newTheme);
		return newTheme;
	});
}

/**
 * Set a specific theme
 */
export function setTheme(newTheme: Theme): void {
	theme.set(newTheme);
	applyTheme(newTheme);
	storeTheme(newTheme);
}

/**
 * Subscribe to system preference changes
 * Returns an unsubscribe function
 */
export function subscribeToSystemPreference(callback: (theme: Theme) => void): () => void {
	if (!isBrowser()) {
		return () => {};
	}

	const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
	const handler = (e: MediaQueryListEvent) => {
		// Only update if there's no stored preference
		if (!getStoredTheme()) {
			const newTheme: Theme = e.matches ? 'dark' : 'light';
			callback(newTheme);
		}
	};

	mediaQuery.addEventListener('change', handler);
	return () => mediaQuery.removeEventListener('change', handler);
}
