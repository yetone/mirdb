/**
 * Unit tests for theme store
 * Owner: Scenario 5 - Theme Toggle
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { get } from 'svelte/store';
import { theme, initTheme, toggleTheme, setTheme } from '$lib/stores/theme';

describe('Theme Store', () => {
	// Mock localStorage
	let localStorageMock: Record<string, string> = {};

	// Mock matchMedia
	let mockMatchMedia: (query: string) => MediaQueryList;
	let darkModePreference = true;

	beforeEach(() => {
		// Reset localStorage mock
		localStorageMock = {};

		// Mock localStorage
		vi.stubGlobal('localStorage', {
			getItem: vi.fn((key: string) => localStorageMock[key] ?? null),
			setItem: vi.fn((key: string, value: string) => {
				localStorageMock[key] = value;
			}),
			removeItem: vi.fn((key: string) => {
				delete localStorageMock[key];
			}),
			clear: vi.fn(() => {
				localStorageMock = {};
			})
		});

		// Mock matchMedia
		mockMatchMedia = vi.fn((query: string) => ({
			matches: query === '(prefers-color-scheme: dark)' ? darkModePreference : false,
			media: query,
			onchange: null,
			addListener: vi.fn(),
			removeListener: vi.fn(),
			addEventListener: vi.fn(),
			removeEventListener: vi.fn(),
			dispatchEvent: vi.fn()
		})) as unknown as (query: string) => MediaQueryList;

		vi.stubGlobal('matchMedia', mockMatchMedia);

		// Mock document
		vi.stubGlobal('document', {
			documentElement: {
				setAttribute: vi.fn(),
				getAttribute: vi.fn()
			}
		});

		// Reset theme store to default
		theme.set('dark');
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	describe('initTheme', () => {
		it('should set theme to dark when system prefers dark mode (test case 2)', () => {
			darkModePreference = true;
			initTheme();

			expect(get(theme)).toBe('dark');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'dark');
		});

		it('should set theme to light when system prefers light mode', () => {
			darkModePreference = false;
			initTheme();

			expect(get(theme)).toBe('light');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'light');
		});

		it('should use stored preference over system preference', () => {
			localStorageMock['mirdb-theme'] = 'light';
			darkModePreference = true; // System prefers dark

			initTheme();

			// Should use stored 'light' instead of system 'dark'
			expect(get(theme)).toBe('light');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'light');
		});
	});

	describe('toggleTheme', () => {
		it('should change theme from light to dark (test case 3)', () => {
			theme.set('light');
			toggleTheme();

			expect(get(theme)).toBe('dark');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'dark');
		});

		it('should change theme from dark to light (test case 4)', () => {
			theme.set('dark');
			toggleTheme();

			expect(get(theme)).toBe('light');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'light');
		});

		it('should persist theme preference to localStorage', () => {
			theme.set('light');
			toggleTheme();

			expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
		});
	});

	describe('setTheme', () => {
		it('should set theme to dark and persist to localStorage (test case 5)', () => {
			setTheme('dark');

			expect(get(theme)).toBe('dark');
			expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'dark');
		});

		it('should set theme to light and persist to localStorage', () => {
			setTheme('light');

			expect(get(theme)).toBe('light');
			expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
			expect(document.documentElement.setAttribute).toHaveBeenCalledWith('data-theme', 'light');
		});
	});

	describe('theme persistence', () => {
		it('should store dark theme in localStorage (test case 5)', () => {
			setTheme('dark');

			expect(localStorageMock['mirdb-theme']).toBe('dark');
		});

		it('should store light theme in localStorage', () => {
			setTheme('light');

			expect(localStorageMock['mirdb-theme']).toBe('light');
		});

		it('should retrieve stored theme on init', () => {
			localStorageMock['mirdb-theme'] = 'dark';
			initTheme();

			expect(get(theme)).toBe('dark');
		});
	});
});
