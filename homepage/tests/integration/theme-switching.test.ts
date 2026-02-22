/**
 * Integration tests for theme switching
 * Owner: Scenario 5 - Theme Toggle
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, fireEvent, cleanup } from '@testing-library/svelte';
import { get } from 'svelte/store';
import ThemeToggle from '$lib/components/ui/ThemeToggle.svelte';
import { theme, initTheme, toggleTheme, setTheme } from '$lib/stores/theme';

describe('Theme Switching Integration', () => {
	// Mock localStorage
	let localStorageMock: Record<string, string> = {};
	let documentSetAttributeSpy: ReturnType<typeof vi.spyOn>;

	beforeEach(() => {
		// Reset localStorage mock
		localStorageMock = {};

		// Mock localStorage
		Object.defineProperty(window, 'localStorage', {
			value: {
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
			},
			writable: true
		});

		// Mock matchMedia with default dark preference
		Object.defineProperty(window, 'matchMedia', {
			value: vi.fn((query: string) => ({
				matches: query === '(prefers-color-scheme: dark)',
				media: query,
				onchange: null,
				addListener: vi.fn(),
				removeListener: vi.fn(),
				addEventListener: vi.fn(),
				removeEventListener: vi.fn(),
				dispatchEvent: vi.fn()
			})),
			writable: true
		});

		// Spy on document.documentElement.setAttribute
		documentSetAttributeSpy = vi.spyOn(document.documentElement, 'setAttribute');

		// Reset theme store
		theme.set('dark');
	});

	afterEach(() => {
		cleanup();
		vi.clearAllMocks();
		documentSetAttributeSpy.mockRestore();
	});

	describe('Theme toggle button integration (test case 6)', () => {
		it('should update document theme class when toggle is clicked', async () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			await fireEvent.click(button);

			// Should have called setAttribute with the new theme
			expect(documentSetAttributeSpy).toHaveBeenCalledWith('data-theme', expect.any(String));
		});

		it('should update theme store when toggle is clicked', async () => {
			// Start with dark theme
			setTheme('dark');

			render(ThemeToggle);
			const button = screen.getByRole('button');

			// Click to toggle to light
			await fireEvent.click(button);

			// Wait for store update
			await vi.waitFor(() => {
				expect(get(theme)).toBe('light');
			});
		});

		it('should toggle back to dark when clicked again', async () => {
			setTheme('light');

			render(ThemeToggle);
			const button = screen.getByRole('button');

			// Click to toggle to dark
			await fireEvent.click(button);

			await vi.waitFor(() => {
				expect(get(theme)).toBe('dark');
			});
		});
	});

	describe('Theme persistence integration', () => {
		it('should persist theme change to localStorage', async () => {
			setTheme('dark');

			render(ThemeToggle);
			const button = screen.getByRole('button');

			await fireEvent.click(button);

			expect(localStorageMock['mirdb-theme']).toBe('light');
		});

		it('should apply theme from localStorage on init', () => {
			localStorageMock['mirdb-theme'] = 'light';

			initTheme();

			expect(get(theme)).toBe('light');
			expect(documentSetAttributeSpy).toHaveBeenCalledWith('data-theme', 'light');
		});
	});

	describe('CSS variables change (test case 6)', () => {
		it('should apply data-theme attribute to documentElement', async () => {
			render(ThemeToggle);

			const button = screen.getByRole('button');
			await fireEvent.click(button);

			// Verify setAttribute was called with data-theme
			expect(documentSetAttributeSpy).toHaveBeenCalledWith('data-theme', expect.any(String));
		});

		it('should toggle between light and dark data-theme values', () => {
			setTheme('dark');

			// Toggle to light
			toggleTheme();
			expect(documentSetAttributeSpy).toHaveBeenCalledWith('data-theme', 'light');

			// Toggle back to dark
			toggleTheme();
			expect(documentSetAttributeSpy).toHaveBeenCalledWith('data-theme', 'dark');
		});
	});

	describe('System preference integration', () => {
		it('should respect system dark preference on first load', () => {
			// Clear any stored preference
			localStorageMock = {};

			initTheme();

			// Should match system preference (dark)
			expect(get(theme)).toBe('dark');
		});

		it('should override system preference with stored preference', () => {
			// Set stored preference to light
			localStorageMock['mirdb-theme'] = 'light';

			initTheme();

			// Should use stored preference instead of system
			expect(get(theme)).toBe('light');
		});
	});
});
