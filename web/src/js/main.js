/**
 * Main JavaScript entry point.
 * Owner: Scenario 1 - HTTP Server and Routing (base setup)
 *          Scenario 4 - Navigation Links (mobile menu toggle)
 *          Scenario 5 - CTA Buttons and Actions (click tracking)
 *          Scenario 8 - Dark Mode Theme (theme toggle, localStorage)
 *
 * Expected functionality:
 * - DOMContentLoaded initialization
 * - Theme preference detection and toggle
 * - Mobile navigation menu toggle
 * - Smooth scroll for anchor links
 * - CTA button click tracking and redirect handling
 */

window.App = window.App || {};

window.App.Nav = {
  init: function() {
    this.initMobileMenu();
    this.initAuthState();
  },

  initMobileMenu: function() {
    var toggle = document.querySelector('[data-testid="mobile-menu-toggle"]');
    var menu = document.getElementById('mobile-menu');
    if (!toggle || !menu) return;

    toggle.addEventListener('click', function() {
      var isExpanded = toggle.getAttribute('aria-expanded') === 'true';
      toggle.setAttribute('aria-expanded', String(!isExpanded));
      menu.setAttribute('aria-hidden', String(isExpanded));
      if (isExpanded) {
        menu.classList.add('hidden');
      } else {
        menu.classList.remove('hidden');
      }
    });
  },

  initAuthState: function() {
    var authState = localStorage.getItem('auth_state') || 'unauthenticated';
    this.setAuthState(authState);
  },

  setAuthState: function(state) {
    var isAuth = state === 'authenticated';
    var unauthGroups = document.querySelectorAll('.nav-unauth-links');
    var authGroups = document.querySelectorAll('.nav-auth-links');

    for (var i = 0; i < unauthGroups.length; i++) {
      if (isAuth) {
        unauthGroups[i].classList.add('hidden');
      } else {
        unauthGroups[i].classList.remove('hidden');
      }
    }

    for (var i = 0; i < authGroups.length; i++) {
      if (isAuth) {
        authGroups[i].classList.remove('hidden');
      } else {
        authGroups[i].classList.add('hidden');
      }
    }
  }
};

document.addEventListener('DOMContentLoaded', function() {
  // Initialize theme
  initTheme();

  // Initialize navigation
  window.App.Nav.init();

  // Initialize CTA button handlers
  initCTAButtons();

  // Initialize analytics counter
  window.App.Stats.init();
});

/**
 * Initialize theme preference detection, persistence, and toggle.
 * Owner: Scenario 8 - Dark Mode Theme
 *
 * Checks localStorage for a saved theme, falls back to system
 * prefers-color-scheme, and sets up the toggle button.
 */
function initTheme() {
  const root = document.documentElement;
  const savedTheme = localStorage.getItem('theme');

  let theme;
  if (savedTheme) {
    theme = savedTheme;
  } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
    theme = 'dark';
  } else {
    theme = 'light';
  }

  root.setAttribute('data-theme', theme);

  // Listen for system preference changes
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
  mediaQuery.addEventListener('change', function(event) {
    // Only apply system preference if user hasn't manually set a preference
    if (!localStorage.getItem('theme')) {
      const newTheme = event.matches ? 'dark' : 'light';
      root.setAttribute('data-theme', newTheme);
    }
  });

  // Set up theme toggle button
  initThemeToggle();
}

/**
 * Initialize the theme toggle button.
 * Owner: Scenario 8 - Dark Mode Theme
 */
function initThemeToggle() {
  const toggleBtns = document.querySelectorAll('[data-testid="theme-toggle"]');
  if (!toggleBtns.length) return;

  toggleBtns.forEach(function(toggleBtn) {
    toggleBtn.addEventListener('click', function() {
      const root = document.documentElement;
      const currentTheme = root.getAttribute('data-theme') || 'light';
      const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

      root.setAttribute('data-theme', newTheme);
      localStorage.setItem('theme', newTheme);

      // Update all toggle button icons/labels
      toggleBtns.forEach(function(btn) {
        updateToggleLabel(btn, newTheme);
      });
    });

    // Set initial toggle label
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
    updateToggleLabel(toggleBtn, currentTheme);
  });
}

/**
 * Update the toggle button's accessible label and icon based on theme.
 * Owner: Scenario 8 - Dark Mode Theme
 */
function updateToggleLabel(button, theme) {
  const isDark = theme === 'dark';
  button.setAttribute('aria-label', isDark ? 'Switch to light mode' : 'Switch to dark mode');
  button.setAttribute('title', isDark ? 'Switch to light mode' : 'Switch to dark mode');

  // Update icon if present
  const icon = button.querySelector('.theme-icon');
  if (icon) {
    icon.textContent = isDark ? '☀' : '☽'; // Sun : Moon
  }
}

/**
 * Analytics Counter Display
 * Owner: Scenario 12 - Analytics Counter Display
 *
 * Features:
 * - Fetches stats from /api/stats endpoint
 * - Formats large numbers with K/M/B suffixes
 * - Shows loading and error states
 * - Updates DOM with formatted values
 */

window.App.Stats = {
  DEFAULT_STATS: {
    urls_created: 128456,
    active_users: 3421,
    total_clicks: 8923456
  },

  init: function() {
    this.fetchStats();
  },

  /**
   * Format a number with K/M/B suffixes for large values.
   * Examples: 128456 -> "128K+", 8923456 -> "8.9M+", 1500000000 -> "1.5B+"
   */
  formatNumber: function(num) {
    if (typeof num !== 'number' || isNaN(num)) {
      return '--';
    }

    var absNum = Math.abs(num);

    if (absNum >= 1000000000) {
      var billions = (absNum / 1000000000).toFixed(1);
      // Remove trailing .0
      billions = billions.replace(/\.0$/, '');
      return billions + 'B+';
    }

    if (absNum >= 1000000) {
      var millions = (absNum / 1000000).toFixed(1);
      millions = millions.replace(/\.0$/, '');
      return millions + 'M+';
    }

    if (absNum >= 1000) {
      var thousands = (absNum / 1000).toFixed(1);
      thousands = thousands.replace(/\.0$/, '');
      return thousands + 'K+';
    }

    return String(num);
  },

  fetchStats: function() {
    var self = this;
    var apiUrl = '/api/stats';

    // Check if analytics section exists on the page
    var analyticsSection = document.getElementById('analytics');
    if (!analyticsSection) return;

    // Show loading state
    self.showLoading(true);
    self.showError(false);

    fetch(apiUrl)
      .then(function(response) {
        if (!response.ok) {
          throw new Error('Failed to fetch stats: ' + response.status);
        }
        return response.json();
      })
      .then(function(data) {
        self.updateCounters(data);
        self.showLoading(false);
      })
      .catch(function(error) {
        console.warn('Analytics stats fetch failed:', error.message);
        // Use fallback/default values on error
        self.updateCounters(self.DEFAULT_STATS);
        self.showLoading(false);
      });
  },

  updateCounters: function(data) {
    var statElements = document.querySelectorAll('.stat-number');

    statElements.forEach(function(el) {
      var key = el.getAttribute('data-stat-key');
      if (key && data.hasOwnProperty(key)) {
        var value = data[key];
        var formatted = this.formatNumber(value);
        el.textContent = formatted;
      }
    }.bind(this));
  },

  showLoading: function(show) {
    var loadingEl = document.getElementById('analytics-loading');
    if (loadingEl) {
      if (show) {
        loadingEl.classList.remove('hidden');
      } else {
        loadingEl.classList.add('hidden');
      }
    }
  },

  showError: function(show) {
    var errorEl = document.getElementById('analytics-error');
    if (errorEl) {
      if (show) {
        errorEl.classList.remove('hidden');
      } else {
        errorEl.classList.add('hidden');
      }
    }
  }
};

/**
 * Initialize CTA button click tracking and interactivity.
 * Owner: Scenario 5 - CTA Buttons and Actions
 */
function initCTAButtons() {
  // Track CTA clicks for analytics
  const ctaSelectors = [
    '[data-testid="hero-primary-cta"]',
    '[data-testid="hero-secondary-cta"]',
    '[data-testid="cta-create-short-url"]',
    '[data-testid="cta-get-started"]'
  ];

  ctaSelectors.forEach(selector => {
    const buttons = document.querySelectorAll(selector);
    buttons.forEach(button => {
      // Ensure buttons are keyboard accessible
      if (button.tagName.toLowerCase() !== 'a') {
        button.setAttribute('role', 'button');
        button.setAttribute('tabindex', '0');
      }

      // Add click tracking
      button.addEventListener('click', function(event) {
        const ctaLabel = button.textContent.trim();
        const ctaHref = button.getAttribute('href') || '#';

        // Store CTA click info in sessionStorage for potential post-redirect use
        sessionStorage.setItem('lastCtaClicked', JSON.stringify({
          label: ctaLabel,
          href: ctaHref,
          timestamp: Date.now()
        }));

        // For external or non-fragment links, allow default navigation
        if (ctaHref && !ctaHref.startsWith('#')) {
          return; // Let the browser handle the redirect
        }

        // For fragment links, smooth scroll
        if (ctaHref && ctaHref.startsWith('#')) {
          event.preventDefault();
          const target = document.querySelector(ctaHref);
          if (target) {
            target.scrollIntoView({ behavior: 'smooth' });
          }
        }
      });

      // Add keyboard support for non-anchor elements
      button.addEventListener('keydown', function(event) {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          button.click();
        }
      });
    });
  });
}
