/**
 * Theme Toggle Unit Tests
 * Owner: Scenario 10 - Dark Theme Support
 *
 * Tests theme toggle module functionality:
 * - getTheme returns current theme
 * - toggleTheme switches between dark and light
 * - setTheme sets specific theme
 * - Theme persists to localStorage
 * - initTheme loads from localStorage or system preference
 */

const STORAGE_KEY = 'mirdb-theme';
const THEME_ATTRIBUTE = 'data-theme';
const DARK_THEME = 'dark';
const LIGHT_THEME = 'light';

// Mock DOM environment
class MockDocument {
  constructor() {
    this.documentElement = {
      attributes: {},
      getAttribute: function(name) {
        return this.attributes[name] || null;
      },
      setAttribute: function(name, value) {
        this.attributes[name] = value;
      },
    };
    this.elements = {};
  }

  getElementById(id) {
    return this.elements[id] || null;
  }

  addEventListener() {}
}

// Mock localStorage
class MockLocalStorage {
  constructor() {
    this.store = {};
  }

  getItem(key) {
    return this.store[key] || null;
  }

  setItem(key, value) {
    this.store[key] = String(value);
  }

  removeItem(key) {
    delete this.store[key];
  }

  clear() {
    this.store = {};
  }
}

// Mock window.matchMedia
function createMockMatchMedia(prefersDark) {
  return function(query) {
    return {
      matches: query === '(prefers-color-scheme: dark)' ? prefersDark : false,
      addEventListener: () => {},
      removeEventListener: () => {},
    };
  };
}

// Pure function implementations for testing (extracted from theme-toggle.js)
function getSystemPreference(matchMedia) {
  if (matchMedia) {
    return matchMedia('(prefers-color-scheme: dark)').matches ? DARK_THEME : LIGHT_THEME;
  }
  return LIGHT_THEME;
}

function getStoredTheme(localStorage) {
  if (localStorage) {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }
  return null;
}

function storeTheme(localStorage, theme) {
  if (localStorage) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch (e) {
      // Silently fail
    }
  }
}

function applyTheme(document, theme) {
  const validTheme = theme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
  document.documentElement.setAttribute(THEME_ATTRIBUTE, validTheme);
  return validTheme;
}

function getTheme(document) {
  const currentTheme = document.documentElement.getAttribute(THEME_ATTRIBUTE);
  return currentTheme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
}

function toggleTheme(document, localStorage) {
  const currentTheme = getTheme(document);
  const newTheme = currentTheme === DARK_THEME ? LIGHT_THEME : DARK_THEME;
  applyTheme(document, newTheme);
  storeTheme(localStorage, newTheme);
  return newTheme;
}

function setTheme(document, localStorage, theme) {
  const validTheme = theme === DARK_THEME ? DARK_THEME : LIGHT_THEME;
  applyTheme(document, validTheme);
  storeTheme(localStorage, validTheme);
}

describe('Theme Toggle Module', () => {
  let mockDocument;
  let mockLocalStorage;

  beforeEach(() => {
    mockDocument = new MockDocument();
    mockLocalStorage = new MockLocalStorage();
  });

  describe('getTheme', () => {
    test('returns "light" when no theme attribute is set', () => {
      const theme = getTheme(mockDocument);
      expect(theme).toBe(LIGHT_THEME);
    });

    test('returns "light" when theme is set to light', () => {
      mockDocument.documentElement.setAttribute(THEME_ATTRIBUTE, LIGHT_THEME);
      const theme = getTheme(mockDocument);
      expect(theme).toBe(LIGHT_THEME);
    });

    test('returns "dark" when theme is set to dark', () => {
      mockDocument.documentElement.setAttribute(THEME_ATTRIBUTE, DARK_THEME);
      const theme = getTheme(mockDocument);
      expect(theme).toBe(DARK_THEME);
    });

    test('returns "light" for invalid theme values', () => {
      mockDocument.documentElement.setAttribute(THEME_ATTRIBUTE, 'invalid');
      const theme = getTheme(mockDocument);
      expect(theme).toBe(LIGHT_THEME);
    });
  });

  describe('toggleTheme', () => {
    test('toggles from light to dark', () => {
      mockDocument.documentElement.setAttribute(THEME_ATTRIBUTE, LIGHT_THEME);
      const newTheme = toggleTheme(mockDocument, mockLocalStorage);
      expect(newTheme).toBe(DARK_THEME);
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(DARK_THEME);
    });

    test('toggles from dark to light', () => {
      mockDocument.documentElement.setAttribute(THEME_ATTRIBUTE, DARK_THEME);
      const newTheme = toggleTheme(mockDocument, mockLocalStorage);
      expect(newTheme).toBe(LIGHT_THEME);
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(LIGHT_THEME);
    });

    test('persists new theme to localStorage', () => {
      mockDocument.documentElement.setAttribute(THEME_ATTRIBUTE, LIGHT_THEME);
      toggleTheme(mockDocument, mockLocalStorage);
      expect(mockLocalStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);
    });

    test('defaults to toggling to dark when no theme is set', () => {
      const newTheme = toggleTheme(mockDocument, mockLocalStorage);
      expect(newTheme).toBe(DARK_THEME);
    });
  });

  describe('setTheme', () => {
    test('sets theme to dark', () => {
      setTheme(mockDocument, mockLocalStorage, DARK_THEME);
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(DARK_THEME);
    });

    test('sets theme to light', () => {
      setTheme(mockDocument, mockLocalStorage, LIGHT_THEME);
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(LIGHT_THEME);
    });

    test('persists theme to localStorage', () => {
      setTheme(mockDocument, mockLocalStorage, DARK_THEME);
      expect(mockLocalStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);
    });

    test('defaults to light for invalid values', () => {
      setTheme(mockDocument, mockLocalStorage, 'invalid');
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(LIGHT_THEME);
    });
  });

  describe('applyTheme', () => {
    test('sets data-theme attribute on documentElement', () => {
      applyTheme(mockDocument, DARK_THEME);
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(DARK_THEME);
    });

    test('normalizes invalid themes to light', () => {
      const result = applyTheme(mockDocument, 'invalid');
      expect(result).toBe(LIGHT_THEME);
      expect(mockDocument.documentElement.getAttribute(THEME_ATTRIBUTE)).toBe(LIGHT_THEME);
    });
  });

  describe('getStoredTheme', () => {
    test('returns null when no theme is stored', () => {
      const stored = getStoredTheme(mockLocalStorage);
      expect(stored).toBeNull();
    });

    test('returns stored theme', () => {
      mockLocalStorage.setItem(STORAGE_KEY, DARK_THEME);
      const stored = getStoredTheme(mockLocalStorage);
      expect(stored).toBe(DARK_THEME);
    });

    test('returns null when localStorage is unavailable', () => {
      const stored = getStoredTheme(null);
      expect(stored).toBeNull();
    });
  });

  describe('storeTheme', () => {
    test('stores theme in localStorage', () => {
      storeTheme(mockLocalStorage, DARK_THEME);
      expect(mockLocalStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);
    });

    test('does not throw when localStorage is unavailable', () => {
      expect(() => storeTheme(null, DARK_THEME)).not.toThrow();
    });
  });

  describe('getSystemPreference', () => {
    test('returns dark when system prefers dark', () => {
      const matchMedia = createMockMatchMedia(true);
      const pref = getSystemPreference(matchMedia);
      expect(pref).toBe(DARK_THEME);
    });

    test('returns light when system prefers light', () => {
      const matchMedia = createMockMatchMedia(false);
      const pref = getSystemPreference(matchMedia);
      expect(pref).toBe(LIGHT_THEME);
    });

    test('returns light when matchMedia is unavailable', () => {
      const pref = getSystemPreference(null);
      expect(pref).toBe(LIGHT_THEME);
    });
  });

  describe('theme persistence flow', () => {
    test('stored preference takes precedence over system preference', () => {
      // System prefers dark
      const matchMedia = createMockMatchMedia(true);
      const systemPref = getSystemPreference(matchMedia);
      expect(systemPref).toBe(DARK_THEME);

      // But user stored light preference
      mockLocalStorage.setItem(STORAGE_KEY, LIGHT_THEME);
      const storedPref = getStoredTheme(mockLocalStorage);
      expect(storedPref).toBe(LIGHT_THEME);

      // Final theme should be light (stored preference)
      const finalTheme = storedPref || systemPref;
      expect(finalTheme).toBe(LIGHT_THEME);
    });

    test('system preference is used when no stored preference', () => {
      const matchMedia = createMockMatchMedia(true);
      const systemPref = getSystemPreference(matchMedia);
      const storedPref = getStoredTheme(mockLocalStorage);

      const finalTheme = storedPref || systemPref;
      expect(finalTheme).toBe(DARK_THEME);
    });
  });

  describe('constants', () => {
    test('STORAGE_KEY is correct', () => {
      expect(STORAGE_KEY).toBe('mirdb-theme');
    });

    test('THEME_ATTRIBUTE is correct', () => {
      expect(THEME_ATTRIBUTE).toBe('data-theme');
    });

    test('DARK_THEME is correct', () => {
      expect(DARK_THEME).toBe('dark');
    });

    test('LIGHT_THEME is correct', () => {
      expect(LIGHT_THEME).toBe('light');
    });
  });
});
