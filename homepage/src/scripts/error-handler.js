/**
 * Error Handler JavaScript
 * Owner: Scenario 16 - Error State Handling
 *
 * Functions:
 * - initImageErrorHandlers(): Add error handlers to images with img-fallback class
 * - handleImageError(event): Handle individual image error
 * - initOfflineDetection(): Detect and indicate offline state
 */

/**
 * Initialize image error handlers for fallback images
 */
export function initImageErrorHandlers() {
  const images = document.querySelectorAll('img.img-fallback');

  images.forEach(img => {
    if (!img.hasAttribute('data-error-bound')) {
      img.addEventListener('error', handleImageError);
      img.setAttribute('data-error-bound', 'true');
    }
  });
}

/**
 * Handle image load error
 * @param {Event} event - The error event
 */
export function handleImageError(event) {
  const img = event.target;

  // Prevent infinite error loops
  if (img.hasAttribute('data-error')) {
    return;
  }

  // Mark as error state
  img.setAttribute('data-error', 'true');
  img.classList.add('img-error');

  // Clear the broken image src to prevent repeated error events
  img.removeAttribute('src');

  // Log for debugging in development
  if (process.env.NODE_ENV !== 'production') {
    console.warn('Image failed to load:', img.alt || 'unnamed image');
  }
}

/**
 * Initialize offline detection
 * Adds 'offline' class to document body when connection is lost
 */
export function initOfflineDetection() {
  const updateOnlineStatus = () => {
    if (navigator.onLine) {
      document.body.classList.remove('offline');
    } else {
      document.body.classList.add('offline');
    }
  };

  window.addEventListener('online', updateOnlineStatus);
  window.addEventListener('offline', updateOnlineStatus);

  // Check initial state
  updateOnlineStatus();
}

/**
 * Global error handler for uncaught errors
 * Prevents white screen of death in production
 */
export function initGlobalErrorHandler() {
  window.addEventListener('error', (event) => {
    // Log error but don't break the page
    console.error('Global error:', event.error);

    // Don't prevent default for image/script loading errors
    // as they are handled separately
  });

  window.addEventListener('unhandledrejection', (event) => {
    console.error('Unhandled promise rejection:', event.reason);
    event.preventDefault(); // Prevent console error spam
  });
}

/**
 * Initialize all error handlers
 */
export function initErrorHandlers() {
  initImageErrorHandlers();
  initOfflineDetection();
  initGlobalErrorHandler();

  // Re-initialize image handlers when new content is added via MutationObserver
  const observer = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      if (mutation.addedNodes.length) {
        initImageErrorHandlers();
      }
    });
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true,
  });
}

// Auto-initialize when DOM is ready
if (typeof document !== 'undefined') {
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initErrorHandlers);
  } else {
    initErrorHandlers();
  }
}
