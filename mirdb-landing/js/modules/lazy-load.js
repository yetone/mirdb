/**
 * Lazy Loading Module
 * Owner: Scenario 18 - Asset Loading and Optimization
 *
 * Handles:
 * - Intersection Observer for lazy loading
 * - Image loading with loading="lazy" fallback
 * - Below-fold content lazy loading
 *
 * Expected exports:
 * - init(): Set up intersection observers
 * - observeElement(element): Add element to observer
 */

/** @type {IntersectionObserver|null} */
let observer = null;

/** Default options for IntersectionObserver */
const defaultOptions = {
  root: null,
  rootMargin: '50px 0px',
  threshold: 0.01
};

/**
 * Handles intersection events for observed elements
 * @param {IntersectionObserverEntry[]} entries
 * @param {IntersectionObserver} observerInstance
 */
const handleIntersection = (entries, observerInstance) => {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      const element = entry.target;

      // Handle images with data-src attribute
      if (element.dataset.src) {
        element.src = element.dataset.src;
        delete element.dataset.src;
      }

      // Handle srcset for responsive images
      if (element.dataset.srcset) {
        element.srcset = element.dataset.srcset;
        delete element.dataset.srcset;
      }

      // Mark element as loaded
      element.classList.add('lazy-loaded');
      element.classList.remove('lazy');

      // Stop observing once loaded
      observerInstance.unobserve(element);
    }
  });
};

/**
 * Creates the IntersectionObserver instance
 * @param {IntersectionObserverInit} [options]
 * @returns {IntersectionObserver|null}
 */
const createObserver = (options = defaultOptions) => {
  if (!('IntersectionObserver' in window)) {
    return null;
  }
  return new IntersectionObserver(handleIntersection, options);
};

/**
 * Fallback loading for browsers without IntersectionObserver support
 * Immediately loads all lazy images
 */
const loadAllImmediately = () => {
  const lazyElements = document.querySelectorAll('[data-src], [data-srcset], .lazy');
  lazyElements.forEach((element) => {
    if (element.dataset.src) {
      element.src = element.dataset.src;
      delete element.dataset.src;
    }
    if (element.dataset.srcset) {
      element.srcset = element.dataset.srcset;
      delete element.dataset.srcset;
    }
    element.classList.add('lazy-loaded');
    element.classList.remove('lazy');
  });
};

/**
 * Add an element to be observed for lazy loading
 * @param {Element} element - The element to observe
 */
export const observeElement = (element) => {
  if (!element) return;

  if (observer) {
    observer.observe(element);
  } else {
    // Fallback: load immediately if observer not available
    if (element.dataset.src) {
      element.src = element.dataset.src;
      delete element.dataset.src;
    }
    if (element.dataset.srcset) {
      element.srcset = element.dataset.srcset;
      delete element.dataset.srcset;
    }
    element.classList.add('lazy-loaded');
    element.classList.remove('lazy');
  }
};

/**
 * Initialize lazy loading functionality
 * Sets up IntersectionObserver and observes all lazy elements
 */
export const init = () => {
  // Check for native lazy loading support
  const supportsNativeLazyLoading = 'loading' in HTMLImageElement.prototype;

  // Create intersection observer
  observer = createObserver();

  if (!observer) {
    // Fallback for browsers without IntersectionObserver
    loadAllImmediately();
    return;
  }

  // Find all elements marked for lazy loading
  const lazyElements = document.querySelectorAll('[data-src], [data-srcset], .lazy');

  lazyElements.forEach((element) => {
    // If native lazy loading is supported and element has loading="lazy",
    // let the browser handle it
    if (supportsNativeLazyLoading && element.getAttribute('loading') === 'lazy') {
      if (element.dataset.src) {
        element.src = element.dataset.src;
        delete element.dataset.src;
      }
      if (element.dataset.srcset) {
        element.srcset = element.dataset.srcset;
        delete element.dataset.srcset;
      }
      element.classList.add('lazy-loaded');
      element.classList.remove('lazy');
    } else {
      // Use IntersectionObserver for custom lazy loading
      observer.observe(element);
    }
  });
};

/**
 * Cleanup function to disconnect the observer
 */
export const destroy = () => {
  if (observer) {
    observer.disconnect();
    observer = null;
  }
};
