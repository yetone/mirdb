/**
 * Unit Tests for Theme Toggle Module
 * Owner: Scenario 6 - Theme Toggle & Accessibility
 *
 * Tests:
 * - getStoredTheme() with various localStorage states
 * - setTheme() persists to localStorage and updates document
 * - getSystemPreference() returns correct system preference
 */

const { resetMocks } = require('../setup');

// Mock matchMedia before requiring the module
let mockMatchMediaResult = false;

Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: jest.fn().mockImplementation(query => ({
        matches: mockMatchMediaResult,
        media: query,
        onchange: null,
        addListener: jest.fn(),
        removeListener: jest.fn(),
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
        dispatchEvent: jest.fn(),
    })),
});

// Now require the theme-toggle module
const {
    getStoredTheme,
    setTheme,
    getSystemPreference,
    getEffectiveTheme,
    applyTheme,
    THEME_STORAGE_KEY
} = require('../../js/theme-toggle');

describe('Theme Toggle Module - Unit Tests', () => {
    beforeEach(() => {
        resetMocks();
        document.documentElement.removeAttribute('data-theme');
        mockMatchMediaResult = false;
    });

    describe('getStoredTheme()', () => {
        test('returns null when localStorage is empty', () => {
            // Test case 6: getStoredTheme() with empty localStorage
            const result = getStoredTheme();
            expect(result).toBeNull();
        });

        test('returns "dark" when localStorage has "dark"', () => {
            // Test case 5: getStoredTheme() with 'dark' in localStorage
            localStorage.setItem(THEME_STORAGE_KEY, 'dark');
            const result = getStoredTheme();
            expect(result).toBe('dark');
        });

        test('returns "light" when localStorage has "light"', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'light');
            const result = getStoredTheme();
            expect(result).toBe('light');
        });

        test('returns null for invalid stored value', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'invalid');
            const result = getStoredTheme();
            expect(result).toBeNull();
        });
    });

    describe('setTheme()', () => {
        test('sets theme to dark and updates localStorage and document', () => {
            // Test case 4: setTheme('dark') updates localStorage and document
            setTheme('dark');

            expect(localStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark');
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
        });

        test('sets theme to light and updates localStorage and document', () => {
            setTheme('light');

            expect(localStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'light');
            expect(document.documentElement.getAttribute('data-theme')).toBe('light');
        });

        test('overwrites existing theme preference', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'light');
            setTheme('dark');

            expect(localStorage.setItem).toHaveBeenLastCalledWith(THEME_STORAGE_KEY, 'dark');
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
        });
    });

    describe('getSystemPreference()', () => {
        test('returns "dark" when system prefers dark mode', () => {
            // Test case 7: getSystemPreference() with prefers-color-scheme: dark
            mockMatchMediaResult = true;
            // Re-mock matchMedia with the new result
            window.matchMedia = jest.fn().mockImplementation(query => ({
                matches: true,
                media: query,
                onchange: null,
                addListener: jest.fn(),
                removeListener: jest.fn(),
                addEventListener: jest.fn(),
                removeEventListener: jest.fn(),
                dispatchEvent: jest.fn(),
            }));

            const result = getSystemPreference();
            expect(result).toBe('dark');
        });

        test('returns "light" when system prefers light mode', () => {
            window.matchMedia = jest.fn().mockImplementation(query => ({
                matches: false,
                media: query,
                onchange: null,
                addListener: jest.fn(),
                removeListener: jest.fn(),
                addEventListener: jest.fn(),
                removeEventListener: jest.fn(),
                dispatchEvent: jest.fn(),
            }));

            const result = getSystemPreference();
            expect(result).toBe('light');
        });
    });

    describe('getEffectiveTheme()', () => {
        test('returns stored theme when available', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'dark');
            const result = getEffectiveTheme();
            expect(result).toBe('dark');
        });

        test('falls back to system preference when no stored theme', () => {
            window.matchMedia = jest.fn().mockImplementation(query => ({
                matches: true,
                media: query,
                onchange: null,
                addListener: jest.fn(),
                removeListener: jest.fn(),
                addEventListener: jest.fn(),
                removeEventListener: jest.fn(),
                dispatchEvent: jest.fn(),
            }));

            const result = getEffectiveTheme();
            expect(result).toBe('dark');
        });
    });

    describe('applyTheme()', () => {
        test('sets data-theme attribute on document', () => {
            applyTheme('dark', false);
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
        });

        test('can apply light theme', () => {
            applyTheme('light', false);
            expect(document.documentElement.getAttribute('data-theme')).toBe('light');
        });

        test('adds transition class when withTransition is true', () => {
            jest.useFakeTimers();
            applyTheme('dark', true);

            expect(document.documentElement.classList.contains('theme-transition')).toBe(true);

            jest.advanceTimersByTime(300);
            expect(document.documentElement.classList.contains('theme-transition')).toBe(false);

            jest.useRealTimers();
        });
    });
});
