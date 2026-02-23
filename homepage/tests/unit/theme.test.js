/**
 * Theme Toggle Unit Tests
 * Owner: Scenario 8 - Theme Toggle
 *
 * Tests:
 * - initTheme() uses system preference when no localStorage
 * - initTheme() uses localStorage preference when available
 * - toggleTheme() switches from light to dark
 * - toggleTheme() switches from dark to light
 * - setTheme() updates DOM and localStorage
 * - getTheme() returns current theme
 */

describe('Theme Toggle Module', () => {
  let MirDBTheme;
  let originalDocument;
  let originalWindow;
  let originalLocalStorage;

  // Create mock implementations
  const createMockDOM = () => {
    const documentElement = {
      _attributes: {},
      getAttribute: jest.fn(function(name) {
        return this._attributes[name] || null;
      }),
      setAttribute: jest.fn(function(name, value) {
        this._attributes[name] = value;
      })
    };

    const mockElement = {
      _attributes: {},
      getAttribute: jest.fn(function(name) {
        return this._attributes[name] || null;
      }),
      setAttribute: jest.fn(function(name, value) {
        this._attributes[name] = value;
      }),
      addEventListener: jest.fn()
    };

    return {
      documentElement,
      querySelector: jest.fn().mockReturnValue(mockElement)
    };
  };

  const createMockLocalStorage = (initialData = {}) => {
    let store = { ...initialData };
    return {
      getItem: jest.fn((key) => store[key] || null),
      setItem: jest.fn((key, value) => {
        store[key] = value;
      }),
      removeItem: jest.fn((key) => {
        delete store[key];
      }),
      clear: jest.fn(() => {
        store = {};
      }),
      _getStore: () => store
    };
  };

  const createMockWindow = (prefersDark = false) => {
    const mediaQuery = {
      matches: prefersDark,
      addEventListener: jest.fn(),
      addListener: jest.fn()
    };

    return {
      matchMedia: jest.fn().mockReturnValue(mediaQuery)
    };
  };

  const setupGlobals = (options = {}) => {
    const {
      prefersDark = false,
      storedTheme = null
    } = options;

    const mockDoc = createMockDOM();
    const mockStore = storedTheme ? { 'mirdb-theme': storedTheme } : {};
    const mockLocalStorage = createMockLocalStorage(mockStore);
    const mockWindow = createMockWindow(prefersDark);

    global.document = mockDoc;
    global.localStorage = mockLocalStorage;
    global.window = mockWindow;

    return { mockDoc, mockLocalStorage, mockWindow };
  };

  beforeEach(() => {
    // Store originals
    originalDocument = global.document;
    originalWindow = global.window;
    originalLocalStorage = global.localStorage;

    // Clear module cache
    jest.resetModules();
    delete global.MirDBTheme;
  });

  afterEach(() => {
    // Restore originals
    global.document = originalDocument;
    global.window = originalWindow;
    global.localStorage = originalLocalStorage;
    delete global.MirDBTheme;
  });

  const loadModule = () => {
    require('../../js/theme.js');
    return global.MirDBTheme;
  };

  describe('initTheme()', () => {
    test('uses localStorage preference when available (dark)', () => {
      const { mockDoc } = setupGlobals({ storedTheme: 'dark', prefersDark: false });

      MirDBTheme = loadModule();
      const result = MirDBTheme.initTheme();

      expect(result).toBe('dark');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('dark');
    });

    test('uses localStorage preference when available (light)', () => {
      const { mockDoc } = setupGlobals({ storedTheme: 'light', prefersDark: true });

      MirDBTheme = loadModule();
      const result = MirDBTheme.initTheme();

      expect(result).toBe('light');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('light');
    });

    test('uses system preference (dark) when no localStorage', () => {
      const { mockDoc } = setupGlobals({ prefersDark: true });

      MirDBTheme = loadModule();
      const result = MirDBTheme.initTheme();

      expect(result).toBe('dark');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('dark');
    });

    test('uses system preference (light) when no localStorage', () => {
      const { mockDoc } = setupGlobals({ prefersDark: false });

      MirDBTheme = loadModule();
      const result = MirDBTheme.initTheme();

      expect(result).toBe('light');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('light');
    });

    test('defaults to light theme when matchMedia is unavailable', () => {
      const { mockDoc, mockWindow } = setupGlobals({ prefersDark: false });
      mockWindow.matchMedia = undefined;
      global.window = mockWindow;

      MirDBTheme = loadModule();
      const result = MirDBTheme.initTheme();

      expect(result).toBe('light');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('light');
    });
  });

  describe('toggleTheme()', () => {
    test('switches from light to dark', () => {
      const { mockLocalStorage } = setupGlobals({ prefersDark: false });

      MirDBTheme = loadModule();

      // Initialize to light
      MirDBTheme.setTheme('light');
      expect(MirDBTheme.getTheme()).toBe('light');

      // Toggle to dark
      const result = MirDBTheme.toggleTheme();

      expect(result).toBe('dark');
      expect(MirDBTheme.getTheme()).toBe('dark');
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    });

    test('switches from dark to light', () => {
      const { mockLocalStorage } = setupGlobals({ prefersDark: true });

      MirDBTheme = loadModule();

      // Initialize to dark
      MirDBTheme.setTheme('dark');
      expect(MirDBTheme.getTheme()).toBe('dark');

      // Toggle to light
      const result = MirDBTheme.toggleTheme();

      expect(result).toBe('light');
      expect(MirDBTheme.getTheme()).toBe('light');
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
    });
  });

  describe('setTheme()', () => {
    test('updates DOM and localStorage for dark theme', () => {
      const { mockDoc, mockLocalStorage } = setupGlobals({ prefersDark: false });

      MirDBTheme = loadModule();
      const result = MirDBTheme.setTheme('dark');

      expect(result).toBe('dark');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('dark');
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    });

    test('updates DOM and localStorage for light theme', () => {
      const { mockDoc, mockLocalStorage } = setupGlobals({ prefersDark: false });

      MirDBTheme = loadModule();
      const result = MirDBTheme.setTheme('light');

      expect(result).toBe('light');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('light');
      expect(mockLocalStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
    });

    test('normalizes invalid theme to light', () => {
      const { mockDoc } = setupGlobals({ prefersDark: false });

      MirDBTheme = loadModule();
      const result = MirDBTheme.setTheme('invalid-theme');

      expect(result).toBe('light');
      expect(mockDoc.documentElement._attributes['data-theme']).toBe('light');
    });
  });

  describe('getTheme()', () => {
    test('returns current theme from DOM', () => {
      setupGlobals({ prefersDark: false });

      MirDBTheme = loadModule();

      MirDBTheme.setTheme('dark');
      expect(MirDBTheme.getTheme()).toBe('dark');

      MirDBTheme.setTheme('light');
      expect(MirDBTheme.getTheme()).toBe('light');
    });

    test('returns light as default when no attribute set', () => {
      const { mockDoc } = setupGlobals({ prefersDark: false });
      // Ensure no attribute is set
      mockDoc.documentElement._attributes = {};
      mockDoc.documentElement.getAttribute = jest.fn(() => null);

      MirDBTheme = loadModule();

      expect(MirDBTheme.getTheme()).toBe('light');
    });
  });

  describe('Module Constants', () => {
    test('exports THEME_KEY constant', () => {
      setupGlobals({ prefersDark: false });
      MirDBTheme = loadModule();

      expect(MirDBTheme.THEME_KEY).toBe('mirdb-theme');
    });

    test('exports THEME_LIGHT constant', () => {
      setupGlobals({ prefersDark: false });
      MirDBTheme = loadModule();

      expect(MirDBTheme.THEME_LIGHT).toBe('light');
    });

    test('exports THEME_DARK constant', () => {
      setupGlobals({ prefersDark: false });
      MirDBTheme = loadModule();

      expect(MirDBTheme.THEME_DARK).toBe('dark');
    });
  });
});
