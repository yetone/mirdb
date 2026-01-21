/**
 * Analytics Tracking Support Tests
 * Scenario: Analytics Tracking Support
 * Description: Verify support for analytics tracking as specified in NFR-6
 */

describe('Analytics Tracking Support', () => {

  /**
   * Test Case 1: Check for analytics script tag
   * Expected: Analytics script (GA, Mixpanel, etc.) is present
   */
  describe('Test Case 1: Analytics Script Presence', () => {
    test('Analytics script tag is present in the document', () => {
      // Check for Google Analytics (gtag.js or analytics.js)
      const gtagScript = document.querySelector('script[src*="googletagmanager.com/gtag"]');
      const gaScript = document.querySelector('script[src*="google-analytics.com/analytics.js"]');
      const ga4Script = document.querySelector('script[src*="googletagmanager.com/gtm.js"]');

      // Check for custom analytics script
      const customAnalyticsScript = document.querySelector('script[data-analytics]');

      // Check for inline analytics configuration
      const inlineAnalytics = Array.from(document.querySelectorAll('script')).some(script => {
        const content = script.textContent || '';
        return content.includes('gtag') ||
               content.includes('ga(') ||
               content.includes('analytics') ||
               content.includes('trackEvent') ||
               content.includes('dataLayer');
      });

      const hasAnalytics = !!(gtagScript || gaScript || ga4Script || customAnalyticsScript || inlineAnalytics);
      expect(hasAnalytics).toBe(true);
    });

    test('Analytics initialization code is present', () => {
      // Check for analytics initialization in scripts
      const scripts = document.querySelectorAll('script');
      let hasAnalyticsInit = false;

      scripts.forEach(script => {
        const content = script.textContent || '';
        const src = script.getAttribute('src') || '';

        // Check for various analytics providers initialization
        if (
          content.includes('gtag(') ||
          content.includes('ga(') ||
          content.includes('mixpanel.init') ||
          content.includes('amplitude.init') ||
          content.includes('analytics.track') ||
          content.includes('trackEvent') ||
          content.includes('initAnalytics') ||
          src.includes('googletagmanager') ||
          src.includes('google-analytics') ||
          src.includes('mixpanel') ||
          src.includes('amplitude')
        ) {
          hasAnalyticsInit = true;
        }
      });

      expect(hasAnalyticsInit).toBe(true);
    });

    test('Analytics tracking ID or configuration is provided', () => {
      const scripts = document.querySelectorAll('script');
      let hasTrackingConfig = false;

      scripts.forEach(script => {
        const content = script.textContent || '';
        const src = script.getAttribute('src') || '';

        // Check for tracking ID patterns
        // GA4: G-XXXXXXXXXX
        // UA: UA-XXXXX-X
        // GTM: GTM-XXXXXX
        if (
          /G-[A-Z0-9]{10}/.test(content) ||
          /UA-\d{4,10}-\d{1,4}/.test(content) ||
          /GTM-[A-Z0-9]{6,8}/.test(content) ||
          /G-[A-Z0-9]{10}/.test(src) ||
          content.includes('MEASUREMENT_ID') ||
          content.includes('trackingId') ||
          content.includes('analytics-config')
        ) {
          hasTrackingConfig = true;
        }
      });

      expect(hasTrackingConfig).toBe(true);
    });
  });

  /**
   * Test Case 2: Verify data attributes for tracking
   * Expected: CTA buttons have tracking data attributes or event handlers
   */
  describe('Test Case 2: CTA Tracking Data Attributes', () => {
    test('Primary CTA button has data attributes for analytics tracking', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      // Check for analytics tracking data attributes
      const hasTrackingAttrs =
        heroCTA.hasAttribute('data-analytics-event') ||
        heroCTA.hasAttribute('data-track') ||
        heroCTA.hasAttribute('data-ga-event') ||
        heroCTA.hasAttribute('data-gtag') ||
        heroCTA.hasAttribute('data-event-category') ||
        heroCTA.hasAttribute('data-analytics') ||
        heroCTA.hasAttribute('data-cta-track');

      expect(hasTrackingAttrs).toBe(true);
    });

    test('Navigation CTA button has data attributes for analytics tracking', () => {
      const navCTA = document.querySelector('[data-testid="nav-cta"]');
      expect(navCTA).toBeInTheDocument();

      const hasTrackingAttrs =
        navCTA.hasAttribute('data-analytics-event') ||
        navCTA.hasAttribute('data-track') ||
        navCTA.hasAttribute('data-ga-event') ||
        navCTA.hasAttribute('data-gtag') ||
        navCTA.hasAttribute('data-event-category') ||
        navCTA.hasAttribute('data-analytics') ||
        navCTA.hasAttribute('data-cta-track');

      expect(hasTrackingAttrs).toBe(true);
    });

    test('Secondary CTA button has data attributes for analytics tracking', () => {
      const secondaryCTA = document.querySelector('[data-testid="hero-cta-secondary"]');
      expect(secondaryCTA).toBeInTheDocument();

      const hasTrackingAttrs =
        secondaryCTA.hasAttribute('data-analytics-event') ||
        secondaryCTA.hasAttribute('data-track') ||
        secondaryCTA.hasAttribute('data-ga-event') ||
        secondaryCTA.hasAttribute('data-gtag') ||
        secondaryCTA.hasAttribute('data-event-category') ||
        secondaryCTA.hasAttribute('data-analytics') ||
        secondaryCTA.hasAttribute('data-cta-track');

      expect(hasTrackingAttrs).toBe(true);
    });

    test('CTA tracking data attributes have meaningful event names', () => {
      const allCTAs = document.querySelectorAll('.cta-button');
      expect(allCTAs.length).toBeGreaterThan(0);

      let hasValidEventNames = true;
      allCTAs.forEach(cta => {
        const eventName =
          cta.getAttribute('data-analytics-event') ||
          cta.getAttribute('data-track') ||
          cta.getAttribute('data-ga-event') ||
          cta.getAttribute('data-analytics');

        // If tracking attribute exists, it should have a meaningful value
        if (eventName) {
          // Event names should describe the action
          const isValidEventName = eventName.length > 0 &&
            !eventName.includes('undefined') &&
            !eventName.includes('null');

          if (!isValidEventName) {
            hasValidEventNames = false;
          }
        }
      });

      expect(hasValidEventNames).toBe(true);
    });

    test('All main CTA buttons are trackable', () => {
      // Get all primary CTAs that should be tracked
      const heroCTAPrimary = document.querySelector('[data-testid="hero-cta-primary"]');
      const heroCTASecondary = document.querySelector('[data-testid="hero-cta-secondary"]');
      const navCTA = document.querySelector('[data-testid="nav-cta"]');

      const trackableCTAs = [heroCTAPrimary, heroCTASecondary, navCTA].filter(cta => cta !== null);

      expect(trackableCTAs.length).toBeGreaterThanOrEqual(3);

      trackableCTAs.forEach(cta => {
        const hasTrackingAttrs =
          cta.hasAttribute('data-analytics-event') ||
          cta.hasAttribute('data-track') ||
          cta.hasAttribute('data-ga-event') ||
          cta.hasAttribute('data-gtag') ||
          cta.hasAttribute('data-event-category') ||
          cta.hasAttribute('data-analytics') ||
          cta.hasAttribute('data-cta-track');

        expect(hasTrackingAttrs).toBe(true);
      });
    });

    test('CTA tracking includes event category or type', () => {
      const heroCTA = document.querySelector('[data-testid="hero-cta-primary"]');
      expect(heroCTA).toBeInTheDocument();

      // Check if tracking includes categorization
      const hasCategory =
        heroCTA.hasAttribute('data-event-category') ||
        heroCTA.hasAttribute('data-analytics-category') ||
        (heroCTA.getAttribute('data-analytics-event') &&
         heroCTA.getAttribute('data-analytics-event').includes('cta'));

      expect(hasCategory).toBe(true);
    });
  });

  /**
   * Additional Analytics Tests
   */
  describe('Analytics Implementation Quality', () => {
    test('Analytics script is loaded appropriately (async or defer)', () => {
      const analyticsScripts = document.querySelectorAll('script[src*="google"], script[data-analytics]');

      // Also check for inline analytics which doesn't need async/defer
      const inlineAnalytics = Array.from(document.querySelectorAll('script')).some(script => {
        const content = script.textContent || '';
        return content.includes('gtag') || content.includes('trackEvent');
      });

      if (analyticsScripts.length > 0) {
        let hasProperLoading = false;
        analyticsScripts.forEach(script => {
          if (script.hasAttribute('async') || script.hasAttribute('defer')) {
            hasProperLoading = true;
          }
        });
        expect(hasProperLoading).toBe(true);
      } else {
        // Inline analytics is acceptable
        expect(inlineAnalytics).toBe(true);
      }
    });

    test('Page view tracking capability exists', () => {
      const scripts = document.querySelectorAll('script');
      let hasPageViewTracking = false;

      scripts.forEach(script => {
        const content = script.textContent || '';
        if (
          content.includes('page_view') ||
          content.includes('pageview') ||
          content.includes('trackPageView') ||
          content.includes("gtag('config'") ||
          content.includes('ga(') ||
          content.includes('send')
        ) {
          hasPageViewTracking = true;
        }
      });

      expect(hasPageViewTracking).toBe(true);
    });

    test('Event tracking function is available for CTA clicks', () => {
      const scripts = document.querySelectorAll('script');
      let hasEventTracking = false;

      scripts.forEach(script => {
        const content = script.textContent || '';
        if (
          content.includes('trackEvent') ||
          content.includes("gtag('event'") ||
          content.includes("ga('send', 'event'") ||
          content.includes('track(') ||
          content.includes('dataLayer.push')
        ) {
          hasEventTracking = true;
        }
      });

      expect(hasEventTracking).toBe(true);
    });
  });
});
