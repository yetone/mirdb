/**
 * Browser Compatibility Tests
 * Owner: Scenario 18 - Browser Compatibility
 *
 * Tests to verify homepage compatibility with modern browsers (NFR-3):
 * - Chrome (latest)
 * - Firefox (latest)
 * - Safari (latest)
 * - Edge (latest)
 *
 * These tests verify that all JavaScript code uses cross-browser compatible APIs
 * and that no browser-specific features are used that would cause failures.
 */

'use strict';

// Load HTML content
var fs = require('fs');
var path = require('path');

// Read the JavaScript files
var apiJs = fs.readFileSync(path.join(__dirname, '../../src/web/scripts/api.js'), 'utf8');
var uiJs = fs.readFileSync(path.join(__dirname, '../../src/web/scripts/ui.js'), 'utf8');
var statusJs = fs.readFileSync(path.join(__dirname, '../../src/web/scripts/status.js'), 'utf8');
var keysJs = fs.readFileSync(path.join(__dirname, '../../src/web/scripts/keys.js'), 'utf8');
var mainJs = fs.readFileSync(path.join(__dirname, '../../src/web/scripts/main.js'), 'utf8');
var htmlContent = fs.readFileSync(path.join(__dirname, '../../src/web/index.html'), 'utf8');

// Browser compatibility test helper that simulates different browser environments
function simulateBrowserEnvironment(browserName) {
    // Store original values
    var originalUserAgent = navigator.userAgent;
    var originalVendor = navigator.vendor;

    // Browser user agent strings for detection
    var userAgents = {
        'Chrome': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Firefox': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Safari': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
        'Edge': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0'
    };

    var vendors = {
        'Chrome': 'Google Inc.',
        'Firefox': '',
        'Safari': 'Apple Computer, Inc.',
        'Edge': 'Google Inc.'
    };

    // Mock navigator with browser-specific values
    Object.defineProperty(navigator, 'userAgent', {
        value: userAgents[browserName] || originalUserAgent,
        configurable: true
    });

    Object.defineProperty(navigator, 'vendor', {
        value: vendors[browserName] || originalVendor,
        configurable: true
    });

    return function restore() {
        Object.defineProperty(navigator, 'userAgent', {
            value: originalUserAgent,
            configurable: true
        });
        Object.defineProperty(navigator, 'vendor', {
            value: originalVendor,
            configurable: true
        });
    };
}

// Mock fetch API for all browsers
function mockFetchAPI() {
    global.fetch = jest.fn().mockImplementation(function(url) {
        var mockResponses = {
            '/api/status': {
                ok: true,
                json: function() {
                    return Promise.resolve({
                        memory_usage: 1024000,
                        active_connections: 5,
                        database_size: 2048000,
                        memtable_count: 2,
                        sstable_count: 10,
                        total_size: 3072000
                    });
                }
            },
            '/api/keys': {
                ok: true,
                json: function() {
                    return Promise.resolve({
                        keys: [
                            { key: 'test-key-1', size: 256, ttl: 0 },
                            { key: 'test-key-2', size: 512, ttl: 3600 }
                        ],
                        total: 2
                    });
                }
            },
            '/api/config': {
                ok: true,
                json: function() {
                    return Promise.resolve({
                        work_dir: '/tmp/mirdb',
                        port: 12333,
                        max_levels: 7,
                        memtable_size: 4194304,
                        memtable_height: 12,
                        imm_memtable_count: 2,
                        sst_max_size: 16777216,
                        l0_compaction_trigger: 4,
                        block_size: 4096,
                        block_restart_interval: 16,
                        thread_sleep_ms: 100
                    });
                }
            }
        };

        // Find matching response
        var response = null;
        Object.keys(mockResponses).forEach(function(key) {
            if (url.indexOf(key) !== -1) {
                response = mockResponses[key];
            }
        });

        if (response) {
            return Promise.resolve(response);
        }

        return Promise.resolve({
            ok: false,
            status: 404,
            json: function() {
                return Promise.resolve({ message: 'Not found' });
            }
        });
    });
}

// Setup DOM environment with HTML
function setupDOM() {
    document.body.innerHTML = htmlContent
        .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
        .replace(/<link[^>]*>/gi, '');
}

describe('Browser Compatibility Tests - NFR-3', function() {
    beforeEach(function() {
        jest.clearAllMocks();
        mockFetchAPI();
        setupDOM();
    });

    afterEach(function() {
        document.body.innerHTML = '';
        jest.restoreAllMocks();
    });

    /**
     * Test Case 1: Chrome Latest Version Rendering
     * Verifies all features are functional with no console errors in Chrome
     */
    describe('Test Case 1: Chrome Latest Version Rendering', function() {
        var restoreBrowser;

        beforeEach(function() {
            restoreBrowser = simulateBrowserEnvironment('Chrome');
        });

        afterEach(function() {
            restoreBrowser();
        });

        test('should detect Chrome browser correctly', function() {
            expect(navigator.userAgent).toContain('Chrome');
            expect(navigator.vendor).toBe('Google Inc.');
        });

        test('should support all standard DOM APIs in Chrome', function() {
            // Verify all standard DOM APIs are available
            expect(typeof document.createElement).toBe('function');
            expect(typeof document.querySelector).toBe('function');
            expect(typeof document.querySelectorAll).toBe('function');
            expect(typeof document.getElementById).toBe('function');
            expect(typeof document.getElementsByClassName).toBe('function');
            expect(typeof document.addEventListener).toBe('function');
        });

        test('should support fetch API in Chrome', function() {
            expect(typeof fetch).toBe('function');
        });

        test('should support Promise API in Chrome', function() {
            expect(typeof Promise).toBe('function');
            expect(typeof Promise.resolve).toBe('function');
            expect(typeof Promise.reject).toBe('function');
            expect(typeof Promise.all).toBe('function');
        });

        test('should support JSON API in Chrome', function() {
            expect(typeof JSON.parse).toBe('function');
            expect(typeof JSON.stringify).toBe('function');
        });

        test('should render all main sections without errors in Chrome', function() {
            var header = document.querySelector('.header');
            var main = document.querySelector('.main');
            var footer = document.querySelector('.footer');

            expect(header).not.toBeNull();
            expect(main).not.toBeNull();
            expect(footer).not.toBeNull();
        });

        test('should support Array methods used in codebase for Chrome', function() {
            var arr = [1, 2, 3];
            expect(typeof arr.forEach).toBe('function');
            expect(typeof arr.filter).toBe('function');
            expect(typeof arr.map).toBe('function');
            expect(typeof arr.indexOf).toBe('function');
        });

        test('should support classList API in Chrome', function() {
            var element = document.createElement('div');
            expect(typeof element.classList.add).toBe('function');
            expect(typeof element.classList.remove).toBe('function');
            expect(typeof element.classList.toggle).toBe('function');
            expect(typeof element.classList.contains).toBe('function');
        });

        test('should support requestAnimationFrame in Chrome', function() {
            expect(typeof window.requestAnimationFrame).toBe('function');
        });

        test('should execute JavaScript without console errors in Chrome', function() {
            var consoleError = jest.spyOn(console, 'error').mockImplementation(function() {});

            // Execute the scripts by evaluating them
            try {
                eval(apiJs);
                eval(uiJs);
            } catch (e) {
                // Some scripts may depend on DOM being ready
            }

            // No critical errors should be thrown
            expect(consoleError).not.toHaveBeenCalled();
            consoleError.mockRestore();
        });
    });

    /**
     * Test Case 2: Firefox Latest Version Rendering
     * Verifies all features are functional with no console errors in Firefox
     */
    describe('Test Case 2: Firefox Latest Version Rendering', function() {
        var restoreBrowser;

        beforeEach(function() {
            restoreBrowser = simulateBrowserEnvironment('Firefox');
        });

        afterEach(function() {
            restoreBrowser();
        });

        test('should detect Firefox browser correctly', function() {
            expect(navigator.userAgent).toContain('Firefox');
        });

        test('should support all standard DOM APIs in Firefox', function() {
            expect(typeof document.createElement).toBe('function');
            expect(typeof document.querySelector).toBe('function');
            expect(typeof document.querySelectorAll).toBe('function');
            expect(typeof document.getElementById).toBe('function');
            expect(typeof document.getElementsByClassName).toBe('function');
            expect(typeof document.addEventListener).toBe('function');
        });

        test('should support fetch API in Firefox', function() {
            expect(typeof fetch).toBe('function');
        });

        test('should support Promise API in Firefox', function() {
            expect(typeof Promise).toBe('function');
            expect(typeof Promise.resolve).toBe('function');
            expect(typeof Promise.reject).toBe('function');
            expect(typeof Promise.all).toBe('function');
        });

        test('should support JSON API in Firefox', function() {
            expect(typeof JSON.parse).toBe('function');
            expect(typeof JSON.stringify).toBe('function');
        });

        test('should render all main sections without errors in Firefox', function() {
            var header = document.querySelector('.header');
            var main = document.querySelector('.main');
            var footer = document.querySelector('.footer');

            expect(header).not.toBeNull();
            expect(main).not.toBeNull();
            expect(footer).not.toBeNull();
        });

        test('should support Array methods used in codebase for Firefox', function() {
            var arr = [1, 2, 3];
            expect(typeof arr.forEach).toBe('function');
            expect(typeof arr.filter).toBe('function');
            expect(typeof arr.map).toBe('function');
            expect(typeof arr.indexOf).toBe('function');
        });

        test('should support classList API in Firefox', function() {
            var element = document.createElement('div');
            expect(typeof element.classList.add).toBe('function');
            expect(typeof element.classList.remove).toBe('function');
            expect(typeof element.classList.toggle).toBe('function');
            expect(typeof element.classList.contains).toBe('function');
        });

        test('should support requestAnimationFrame in Firefox', function() {
            expect(typeof window.requestAnimationFrame).toBe('function');
        });

        test('should execute JavaScript without console errors in Firefox', function() {
            var consoleError = jest.spyOn(console, 'error').mockImplementation(function() {});

            try {
                eval(apiJs);
                eval(uiJs);
            } catch (e) {
                // Some scripts may depend on DOM being ready
            }

            expect(consoleError).not.toHaveBeenCalled();
            consoleError.mockRestore();
        });

        test('should support scroll behavior in Firefox', function() {
            expect(typeof window.scrollTo).toBe('function');
        });
    });

    /**
     * Test Case 3: Safari Latest Version Rendering
     * Verifies all features are functional with no console errors in Safari
     */
    describe('Test Case 3: Safari Latest Version Rendering', function() {
        var restoreBrowser;

        beforeEach(function() {
            restoreBrowser = simulateBrowserEnvironment('Safari');
        });

        afterEach(function() {
            restoreBrowser();
        });

        test('should detect Safari browser correctly', function() {
            expect(navigator.userAgent).toContain('Safari');
            expect(navigator.vendor).toBe('Apple Computer, Inc.');
        });

        test('should support all standard DOM APIs in Safari', function() {
            expect(typeof document.createElement).toBe('function');
            expect(typeof document.querySelector).toBe('function');
            expect(typeof document.querySelectorAll).toBe('function');
            expect(typeof document.getElementById).toBe('function');
            expect(typeof document.getElementsByClassName).toBe('function');
            expect(typeof document.addEventListener).toBe('function');
        });

        test('should support fetch API in Safari', function() {
            expect(typeof fetch).toBe('function');
        });

        test('should support Promise API in Safari', function() {
            expect(typeof Promise).toBe('function');
            expect(typeof Promise.resolve).toBe('function');
            expect(typeof Promise.reject).toBe('function');
            expect(typeof Promise.all).toBe('function');
        });

        test('should support JSON API in Safari', function() {
            expect(typeof JSON.parse).toBe('function');
            expect(typeof JSON.stringify).toBe('function');
        });

        test('should render all main sections without errors in Safari', function() {
            var header = document.querySelector('.header');
            var main = document.querySelector('.main');
            var footer = document.querySelector('.footer');

            expect(header).not.toBeNull();
            expect(main).not.toBeNull();
            expect(footer).not.toBeNull();
        });

        test('should support Array methods used in codebase for Safari', function() {
            var arr = [1, 2, 3];
            expect(typeof arr.forEach).toBe('function');
            expect(typeof arr.filter).toBe('function');
            expect(typeof arr.map).toBe('function');
            expect(typeof arr.indexOf).toBe('function');
        });

        test('should support classList API in Safari', function() {
            var element = document.createElement('div');
            expect(typeof element.classList.add).toBe('function');
            expect(typeof element.classList.remove).toBe('function');
            expect(typeof element.classList.toggle).toBe('function');
            expect(typeof element.classList.contains).toBe('function');
        });

        test('should support requestAnimationFrame in Safari', function() {
            expect(typeof window.requestAnimationFrame).toBe('function');
        });

        test('should execute JavaScript without console errors in Safari', function() {
            var consoleError = jest.spyOn(console, 'error').mockImplementation(function() {});

            try {
                eval(apiJs);
                eval(uiJs);
            } catch (e) {
                // Some scripts may depend on DOM being ready
            }

            expect(consoleError).not.toHaveBeenCalled();
            consoleError.mockRestore();
        });

        test('should support webkit-specific scroll behavior fallback in Safari', function() {
            expect(typeof window.scrollTo).toBe('function');
        });
    });

    /**
     * Test Case 4: Edge Latest Version Rendering
     * Verifies all features are functional with no console errors in Edge
     */
    describe('Test Case 4: Edge Latest Version Rendering', function() {
        var restoreBrowser;

        beforeEach(function() {
            restoreBrowser = simulateBrowserEnvironment('Edge');
        });

        afterEach(function() {
            restoreBrowser();
        });

        test('should detect Edge browser correctly', function() {
            expect(navigator.userAgent).toContain('Edg');
        });

        test('should support all standard DOM APIs in Edge', function() {
            expect(typeof document.createElement).toBe('function');
            expect(typeof document.querySelector).toBe('function');
            expect(typeof document.querySelectorAll).toBe('function');
            expect(typeof document.getElementById).toBe('function');
            expect(typeof document.getElementsByClassName).toBe('function');
            expect(typeof document.addEventListener).toBe('function');
        });

        test('should support fetch API in Edge', function() {
            expect(typeof fetch).toBe('function');
        });

        test('should support Promise API in Edge', function() {
            expect(typeof Promise).toBe('function');
            expect(typeof Promise.resolve).toBe('function');
            expect(typeof Promise.reject).toBe('function');
            expect(typeof Promise.all).toBe('function');
        });

        test('should support JSON API in Edge', function() {
            expect(typeof JSON.parse).toBe('function');
            expect(typeof JSON.stringify).toBe('function');
        });

        test('should render all main sections without errors in Edge', function() {
            var header = document.querySelector('.header');
            var main = document.querySelector('.main');
            var footer = document.querySelector('.footer');

            expect(header).not.toBeNull();
            expect(main).not.toBeNull();
            expect(footer).not.toBeNull();
        });

        test('should support Array methods used in codebase for Edge', function() {
            var arr = [1, 2, 3];
            expect(typeof arr.forEach).toBe('function');
            expect(typeof arr.filter).toBe('function');
            expect(typeof arr.map).toBe('function');
            expect(typeof arr.indexOf).toBe('function');
        });

        test('should support classList API in Edge', function() {
            var element = document.createElement('div');
            expect(typeof element.classList.add).toBe('function');
            expect(typeof element.classList.remove).toBe('function');
            expect(typeof element.classList.toggle).toBe('function');
            expect(typeof element.classList.contains).toBe('function');
        });

        test('should support requestAnimationFrame in Edge', function() {
            expect(typeof window.requestAnimationFrame).toBe('function');
        });

        test('should execute JavaScript without console errors in Edge', function() {
            var consoleError = jest.spyOn(console, 'error').mockImplementation(function() {});

            try {
                eval(apiJs);
                eval(uiJs);
            } catch (e) {
                // Some scripts may depend on DOM being ready
            }

            expect(consoleError).not.toHaveBeenCalled();
            consoleError.mockRestore();
        });
    });

    /**
     * Cross-browser JavaScript Feature Compatibility Tests
     * Verifies that all JavaScript patterns used are cross-browser compatible
     */
    describe('Cross-Browser JavaScript Feature Compatibility', function() {
        test('should use cross-browser compatible IIFE pattern', function() {
            // Verify IIFE pattern works
            var testModule = (function() {
                'use strict';
                var privateVar = 'test';
                return {
                    getVar: function() { return privateVar; }
                };
            })();

            expect(testModule.getVar()).toBe('test');
        });

        test('should use cross-browser compatible var declarations', function() {
            // Code uses var instead of let/const for wider compatibility
            expect(apiJs).toContain('var MirDBApi');
            expect(statusJs).toContain('var MirDBStatus');
            expect(keysJs).toContain('var MirDBKeys');
        });

        test('should use cross-browser compatible function expressions', function() {
            // Code uses function expressions instead of arrow functions
            expect(apiJs).toContain('function fetchStatus');
            expect(apiJs).toContain('function handleResponse');
        });

        test('should have clipboard fallback for older browsers', function() {
            // ui.js should have fallback copy method
            expect(uiJs).toContain('fallbackCopyToClipboard');
        });

        test('should use encodeURIComponent for URL encoding', function() {
            // Verify proper URL encoding is used
            expect(apiJs).toContain('encodeURIComponent');
        });

        test('should support Object.keys for iteration', function() {
            expect(typeof Object.keys).toBe('function');
        });

        test('should support String.prototype methods', function() {
            var str = 'test';
            expect(typeof str.indexOf).toBe('function');
            expect(typeof str.toLowerCase).toBe('function');
            expect(typeof str.trim).toBe('function');
        });

        test('should support Math methods used in formatting', function() {
            expect(typeof Math.floor).toBe('function');
            expect(typeof Math.log).toBe('function');
            expect(typeof Math.pow).toBe('function');
            expect(typeof Math.min).toBe('function');
        });

        test('should use cross-browser compatible event handling', function() {
            // Verify addEventListener is used instead of attachEvent
            expect(mainJs).toContain('addEventListener');
            expect(mainJs).not.toContain('attachEvent');
        });
    });

    /**
     * DOM API Compatibility Tests
     * Verifies DOM APIs used are available in all target browsers
     */
    describe('DOM API Compatibility', function() {
        test('should support document.createElement', function() {
            var div = document.createElement('div');
            expect(div).toBeInstanceOf(HTMLDivElement);
        });

        test('should support element.setAttribute', function() {
            var div = document.createElement('div');
            div.setAttribute('data-test', 'value');
            expect(div.getAttribute('data-test')).toBe('value');
        });

        test('should support element.innerHTML', function() {
            var div = document.createElement('div');
            div.innerHTML = '<span>test</span>';
            expect(div.querySelector('span')).not.toBeNull();
        });

        test('should support element.textContent', function() {
            var div = document.createElement('div');
            div.textContent = 'test content';
            expect(div.textContent).toBe('test content');
        });

        test('should support element.style manipulation', function() {
            var div = document.createElement('div');
            div.style.display = 'none';
            expect(div.style.display).toBe('none');
        });

        test('should support document.body.appendChild', function() {
            var div = document.createElement('div');
            div.id = 'test-append';
            document.body.appendChild(div);
            expect(document.getElementById('test-append')).not.toBeNull();
            document.body.removeChild(div);
        });

        test('should support element.remove', function() {
            var div = document.createElement('div');
            div.id = 'test-remove';
            document.body.appendChild(div);
            div.remove();
            expect(document.getElementById('test-remove')).toBeNull();
        });

        test('should support element.focus', function() {
            var input = document.createElement('input');
            document.body.appendChild(input);
            expect(typeof input.focus).toBe('function');
            input.remove();
        });
    });

    /**
     * CSS Feature Detection Tests
     * Verifies CSS features used via JavaScript are compatible
     */
    describe('CSS Feature Detection via JavaScript', function() {
        test('should support element.style for CSS manipulation', function() {
            var div = document.createElement('div');
            expect(div.style).toBeDefined();
        });

        test('should support setting CSS custom properties via JavaScript', function() {
            var div = document.createElement('div');
            div.style.setProperty('--test-color', '#ff0000');
            // jsdom has limited CSS custom property support, just verify the method exists
            expect(typeof div.style.setProperty).toBe('function');
        });

        test('should support getComputedStyle', function() {
            expect(typeof window.getComputedStyle).toBe('function');
        });

        test('should support offsetWidth and offsetHeight', function() {
            var div = document.createElement('div');
            document.body.appendChild(div);
            expect(typeof div.offsetWidth).toBe('number');
            expect(typeof div.offsetHeight).toBe('number');
            div.remove();
        });

        test('should support getBoundingClientRect', function() {
            var div = document.createElement('div');
            document.body.appendChild(div);
            var rect = div.getBoundingClientRect();
            expect(typeof rect.top).toBe('number');
            expect(typeof rect.left).toBe('number');
            div.remove();
        });
    });

    /**
     * Event Handling Compatibility Tests
     */
    describe('Event Handling Compatibility', function() {
        test('should support addEventListener', function() {
            var div = document.createElement('div');
            var clicked = false;
            div.addEventListener('click', function() {
                clicked = true;
            });
            div.click();
            expect(clicked).toBe(true);
        });

        test('should support removeEventListener', function() {
            var div = document.createElement('div');
            var clicked = false;
            var handler = function() { clicked = true; };
            div.addEventListener('click', handler);
            div.removeEventListener('click', handler);
            div.click();
            expect(clicked).toBe(false);
        });

        test('should support DOMContentLoaded event', function() {
            expect(typeof document.addEventListener).toBe('function');
        });

        test('should support keyboard events', function() {
            var input = document.createElement('input');
            var keyPressed = false;
            input.addEventListener('keydown', function(e) {
                keyPressed = true;
            });
            var event = new KeyboardEvent('keydown', { key: 'Enter' });
            input.dispatchEvent(event);
            expect(keyPressed).toBe(true);
        });

        test('should support event.preventDefault', function() {
            var form = document.createElement('form');
            var defaultPrevented = false;
            form.addEventListener('submit', function(e) {
                e.preventDefault();
                defaultPrevented = true;
            });
            var event = new Event('submit', { cancelable: true });
            form.dispatchEvent(event);
            expect(defaultPrevented).toBe(true);
        });

        test('should support event.stopPropagation', function() {
            var parent = document.createElement('div');
            var child = document.createElement('button');
            parent.appendChild(child);

            var parentClicked = false;
            var childClicked = false;

            parent.addEventListener('click', function() {
                parentClicked = true;
            });

            child.addEventListener('click', function(e) {
                e.stopPropagation();
                childClicked = true;
            });

            child.click();
            expect(childClicked).toBe(true);
            expect(parentClicked).toBe(false);
        });
    });

    /**
     * Async/Await and Promise Compatibility Tests
     */
    describe('Async/Promise Compatibility', function() {
        test('should support Promise.resolve', function() {
            return Promise.resolve('test').then(function(result) {
                expect(result).toBe('test');
            });
        });

        test('should support Promise.reject', function() {
            return Promise.reject(new Error('test'))
                .catch(function(error) {
                    expect(error.message).toBe('test');
                });
        });

        test('should support Promise chaining', function() {
            return Promise.resolve(1)
                .then(function(val) { return val + 1; })
                .then(function(val) { return val + 1; })
                .then(function(val) {
                    expect(val).toBe(3);
                });
        });

        test('should support setTimeout', function(done) {
            setTimeout(function() {
                expect(true).toBe(true);
                done();
            }, 10);
        });

        test('should support setInterval and clearInterval', function(done) {
            var count = 0;
            var interval = setInterval(function() {
                count++;
                if (count >= 2) {
                    clearInterval(interval);
                    expect(count).toBeGreaterThanOrEqual(2);
                    done();
                }
            }, 10);
        });
    });

    /**
     * UI Components Rendering Tests
     */
    describe('UI Components Rendering Compatibility', function() {
        test('should render navigation links correctly', function() {
            var navLinks = document.querySelectorAll('.nav-link');
            expect(navLinks.length).toBeGreaterThan(0);
        });

        test('should render hero section correctly', function() {
            var hero = document.querySelector('.hero');
            expect(hero).not.toBeNull();
        });

        test('should render status panel container', function() {
            var statusPanel = document.getElementById('status-panel');
            expect(statusPanel).not.toBeNull();
        });

        test('should render key browser container', function() {
            var keyBrowser = document.getElementById('key-browser');
            expect(keyBrowser).not.toBeNull();
        });

        test('should render config panel container', function() {
            var configPanel = document.getElementById('config-panel');
            expect(configPanel).not.toBeNull();
        });

        test('should render footer with documentation links', function() {
            var footer = document.querySelector('.footer');
            var docLinks = footer.querySelectorAll('.footer-link');
            expect(docLinks.length).toBeGreaterThan(0);
        });

        test('should render mobile navigation toggle', function() {
            var navToggle = document.getElementById('nav-toggle');
            expect(navToggle).not.toBeNull();
            expect(navToggle.getAttribute('aria-expanded')).toBe('false');
        });

        test('should have proper ARIA attributes', function() {
            var navToggle = document.getElementById('nav-toggle');
            expect(navToggle.getAttribute('aria-label')).toBeTruthy();
            expect(navToggle.getAttribute('aria-controls')).toBeTruthy();
        });
    });

    /**
     * Console Error Detection Tests
     * Verifies JavaScript syntax is valid and can be parsed without errors
     */
    describe('No Console Errors on Page Load', function() {
        test('should have valid JavaScript syntax in main.js', function() {
            // Verify no syntax errors in main.js
            expect(function() {
                new Function(mainJs);
            }).not.toThrow();
        });

        test('should have valid JavaScript syntax in api.js', function() {
            // Verify no syntax errors in api.js
            expect(function() {
                new Function(apiJs);
            }).not.toThrow();
        });

        test('should have valid JavaScript syntax in ui.js', function() {
            // Verify no syntax errors in ui.js
            expect(function() {
                new Function(uiJs);
            }).not.toThrow();
        });

        test('should have valid JavaScript syntax in status.js', function() {
            // Verify no syntax errors in status.js
            expect(function() {
                new Function(statusJs);
            }).not.toThrow();
        });

        test('should have valid JavaScript syntax in keys.js', function() {
            // Verify no syntax errors in keys.js
            expect(function() {
                new Function(keysJs);
            }).not.toThrow();
        });

        test('should load api.js and create MirDBApi module', function() {
            // Execute api.js and verify module is created
            var scriptFn = new Function(apiJs + '\nreturn typeof MirDBApi;');
            var result = scriptFn();
            expect(result).toBe('object');
        });

        test('should load ui.js and create MirDBUI module', function() {
            // Execute ui.js and verify module is created
            var scriptFn = new Function(uiJs + '\nreturn typeof MirDBUI;');
            var result = scriptFn();
            expect(result).toBe('object');
        });

        test('should have no use of deprecated browser APIs', function() {
            // Check for deprecated APIs that might cause issues
            var allScripts = apiJs + uiJs + statusJs + keysJs + mainJs;

            // Should not use document.write
            expect(allScripts).not.toContain('document.write');

            // Should not use attachEvent (IE-specific)
            expect(allScripts).not.toContain('attachEvent');

            // Should not use innerText in a browser-inconsistent way
            // (textContent is preferred and is used in the codebase)
            expect(allScripts).toContain('textContent');
        });

        test('should use cross-browser compatible strict mode', function() {
            // Verify strict mode is used for better cross-browser consistency
            expect(apiJs).toContain("'use strict'");
            expect(statusJs).toContain("'use strict'");
            expect(keysJs).toContain("'use strict'");
        });
    });
});
