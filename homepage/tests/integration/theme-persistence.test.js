/**
 * Integration Tests for Theme Persistence
 * Owner: Scenario 6 - Theme Toggle & Accessibility
 *
 * Tests:
 * - Theme persists across page loads via localStorage
 * - System preference fallback when no stored preference
 * - Page loads without flash of wrong theme
 */

const { resetMocks } = require('../setup');

// Mock matchMedia
let mockMatchMediaResult = false;
let mockMatchMediaListeners = [];

Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: jest.fn().mockImplementation(query => ({
        matches: mockMatchMediaResult,
        media: query,
        onchange: null,
        addListener: jest.fn(callback => mockMatchMediaListeners.push(callback)),
        removeListener: jest.fn(),
        addEventListener: jest.fn((event, callback) => mockMatchMediaListeners.push(callback)),
        removeEventListener: jest.fn(),
        dispatchEvent: jest.fn(),
    })),
});

const {
    getStoredTheme,
    setTheme,
    getSystemPreference,
    getEffectiveTheme,
    initThemeToggle,
    THEME_STORAGE_KEY
} = require('../../js/theme-toggle');

describe('Theme Persistence - Integration Tests', () => {
    beforeEach(() => {
        resetMocks();
        mockMatchMediaListeners = [];
        document.documentElement.removeAttribute('data-theme');
        document.body.innerHTML = '';
        mockMatchMediaResult = false;

        // Reset matchMedia mock
        window.matchMedia = jest.fn().mockImplementation(query => ({
            matches: mockMatchMediaResult,
            media: query,
            onchange: null,
            addListener: jest.fn(callback => mockMatchMediaListeners.push(callback)),
            removeListener: jest.fn(),
            addEventListener: jest.fn((event, callback) => mockMatchMediaListeners.push(callback)),
            removeEventListener: jest.fn(),
            dispatchEvent: jest.fn(),
        }));
    });

    describe('Page Load with Stored Theme', () => {
        test('loads page in dark mode when theme="dark" is in localStorage', () => {
            // Test case 8: Load page with theme='dark' in localStorage
            // Simulate stored dark theme
            localStorage.setItem(THEME_STORAGE_KEY, 'dark');

            // Initialize (simulating page load)
            initThemeToggle();

            // Verify dark mode is applied immediately
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

            // Verify no transition class on initial load
            expect(document.documentElement.classList.contains('theme-transition')).toBe(false);
        });

        test('loads page in light mode when theme="light" is in localStorage', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'light');

            initThemeToggle();

            expect(document.documentElement.getAttribute('data-theme')).toBe('light');
        });
    });

    describe('System Preference Fallback', () => {
        test('loads page in dark mode following system preference when no stored theme', () => {
            // Test case 9: Load page with no stored theme, system prefers dark
            mockMatchMediaResult = true;
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

            // No stored theme
            expect(getStoredTheme()).toBeNull();

            initThemeToggle();

            // Should follow system preference (dark)
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
        });

        test('loads page in light mode following system preference when no stored theme', () => {
            mockMatchMediaResult = false;
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

            // No stored theme
            expect(getStoredTheme()).toBeNull();

            initThemeToggle();

            // Should follow system preference (light)
            expect(document.documentElement.getAttribute('data-theme')).toBe('light');
        });
    });

    describe('Theme Toggle Button Integration', () => {
        test('creates theme toggle button when initialized', () => {
            initThemeToggle();

            const button = document.querySelector('.theme-toggle');
            expect(button).not.toBeNull();
            expect(button.getAttribute('aria-label')).toBe('Toggle dark/light theme');
        });

        test('clicking toggle button changes theme and persists', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'light');
            initThemeToggle();

            const button = document.querySelector('.theme-toggle');
            expect(document.documentElement.getAttribute('data-theme')).toBe('light');

            // Click to toggle to dark
            button.click();

            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
            expect(localStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark');
        });

        test('toggle button works both directions', () => {
            localStorage.setItem(THEME_STORAGE_KEY, 'light');
            initThemeToggle();

            const button = document.querySelector('.theme-toggle');

            // Light -> Dark
            button.click();
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

            // Dark -> Light
            button.click();
            expect(document.documentElement.getAttribute('data-theme')).toBe('light');
        });
    });

    describe('Session Persistence Simulation', () => {
        test('theme persists across simulated page reloads', () => {
            // First "page load" - user sets theme
            initThemeToggle();
            setTheme('dark');
            expect(localStorage.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark');

            // Simulate page unload/reload by clearing DOM state
            document.body.innerHTML = '';
            document.documentElement.removeAttribute('data-theme');

            // Second "page load" - theme should be restored
            // Re-mock localStorage.getItem to return the stored value
            localStorage.getItem.mockReturnValue('dark');

            initThemeToggle();
            expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
        });
    });
});
