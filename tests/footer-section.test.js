/**
 * Footer Section Tests
 * Tests for REQ-4: Footer with contact information, social links, and secondary navigation
 * Scenario: Footer Section Structure
 */

describe('Footer Section Structure', () => {

  // Test Case 1: Check footer element exists
  describe('Test Case 1: Footer Element Exists', () => {
    test('Footer element is present at page bottom', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Footer should be the last major element before closing body
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('Footer has proper semantic structure', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Footer should contain organized content sections
      const footerContent = footer.querySelector('.footer-content');
      expect(footerContent).toBeInTheDocument();
    });
  });

  // Test Case 2: Verify contact information presence
  describe('Test Case 2: Contact Information Presence', () => {
    test('At least one contact method (email, phone, or address) is displayed', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Check for email (mailto: links or email pattern)
      const emailLinks = footer.querySelectorAll('a[href^="mailto:"]');
      const emailPattern = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/;
      const hasEmailLink = emailLinks.length > 0;
      const hasEmailText = emailPattern.test(footer.textContent);

      // Check for phone (tel: links or phone pattern)
      const phoneLinks = footer.querySelectorAll('a[href^="tel:"]');
      const phonePattern = /(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})|(\+\d{1,3}[-.\s]?\d{1,14})/;
      const hasPhoneLink = phoneLinks.length > 0;
      const hasPhoneText = phonePattern.test(footer.textContent);

      // Check for address section
      const addressElement = footer.querySelector('[data-testid="footer-address"], .footer-address, address');
      const hasAddress = addressElement !== null;

      // At least one contact method must be present
      const hasContactInfo = hasEmailLink || hasEmailText || hasPhoneLink || hasPhoneText || hasAddress;
      expect(hasContactInfo).toBe(true);
    });

    test('Contact information is in a dedicated section', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Look for contact section
      const contactSection = footer.querySelector('[data-testid="footer-contact"], .footer-contact, .footer-section');
      expect(contactSection).toBeInTheDocument();
    });
  });

  // Test Case 3: Check social media links
  describe('Test Case 3: Social Media Links', () => {
    test('At least 2 social media icons with valid href attributes', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Look for social media container
      const socialSection = footer.querySelector('[data-testid="footer-social"], .footer-social, .social-links');
      expect(socialSection).toBeInTheDocument();

      // Find social links - they typically link to major social platforms
      const socialPatterns = [
        /github\.com/i,
        /twitter\.com/i,
        /x\.com/i,
        /linkedin\.com/i,
        /facebook\.com/i,
        /instagram\.com/i,
        /youtube\.com/i,
        /discord\.com/i,
        /discord\.gg/i,
        /mastodon/i
      ];

      const allLinks = socialSection.querySelectorAll('a[href]');
      const socialLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href');
        return socialPatterns.some(pattern => pattern.test(href));
      });

      expect(socialLinks.length).toBeGreaterThanOrEqual(2);
    });

    test('Social links have icons or recognizable content', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      const socialSection = footer.querySelector('[data-testid="footer-social"], .footer-social, .social-links');
      expect(socialSection).toBeInTheDocument();

      const socialLinks = socialSection.querySelectorAll('a[href]');

      socialLinks.forEach(link => {
        // Each link should have either an icon (svg, img, i) or text content
        const hasIcon = link.querySelector('svg, img, i') !== null;
        const hasText = link.textContent.trim().length > 0;
        const hasAriaLabel = link.hasAttribute('aria-label');

        expect(hasIcon || hasText || hasAriaLabel).toBe(true);
      });
    });
  });

  // Test Case 4: Verify secondary navigation links
  describe('Test Case 4: Secondary Navigation Links', () => {
    test('Footer contains navigation links to other pages/sections', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Find all internal navigation links (not social/external)
      const footerLinks = footer.querySelectorAll('.footer-links a, .footer-nav a, .footer-section a');

      // Filter for navigation-type links (internal pages or sections)
      const navLinks = Array.from(footerLinks).filter(link => {
        const href = link.getAttribute('href');
        if (!href) return false;

        // Include internal links (#section), relative paths, or same-domain links
        const isInternalSection = href.startsWith('#');
        const isRelativePath = !href.startsWith('http') && !href.startsWith('mailto:') && !href.startsWith('tel:');

        return isInternalSection || isRelativePath;
      });

      // Should have at least some navigation links
      expect(navLinks.length).toBeGreaterThanOrEqual(1);
    });

    test('Footer has organized link sections', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Footer should have multiple sections for organization
      const sections = footer.querySelectorAll('.footer-section, .footer-column');
      expect(sections.length).toBeGreaterThanOrEqual(2);
    });
  });

  // Test Case 5: Check copyright text
  describe('Test Case 5: Copyright Notice', () => {
    test('Copyright notice is present with current year', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      // Look for copyright section
      const footerBottom = footer.querySelector('.footer-bottom, [data-testid="footer-copyright"]');
      expect(footerBottom).toBeInTheDocument();

      // Check for copyright text
      const footerText = footerBottom.textContent.toLowerCase();
      const hasCopyrightSymbol = footerText.includes('©') || footerText.includes('copyright');

      expect(hasCopyrightSymbol).toBe(true);

      // Check for current year (2024 or 2025 or 2026)
      const currentYear = new Date().getFullYear().toString();
      const hasYear = footerText.includes(currentYear) ||
                      footerText.includes('2024') ||
                      footerText.includes('2025') ||
                      footerText.includes('2026');
      expect(hasYear).toBe(true);
    });

    test('Copyright section contains legal information', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      const footerBottom = footer.querySelector('.footer-bottom, [data-testid="footer-copyright"]');

      expect(footerBottom).toBeInTheDocument();

      // Should have some legal text (copyright, license, rights)
      const legalKeywords = ['©', 'copyright', 'license', 'rights', 'reserved', 'mit', 'apache'];
      const bottomText = footerBottom.textContent.toLowerCase();
      const hasLegalInfo = legalKeywords.some(keyword => bottomText.includes(keyword));

      expect(hasLegalInfo).toBe(true);
    });
  });

  // Test Case 6: Test social link behavior
  describe('Test Case 6: Social Link Behavior', () => {
    test('Social links open in new tab (target="_blank")', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      const socialSection = footer.querySelector('[data-testid="footer-social"], .footer-social, .social-links');
      expect(socialSection).toBeInTheDocument();

      // Find external social links
      const socialPatterns = [
        /github\.com/i,
        /twitter\.com/i,
        /x\.com/i,
        /linkedin\.com/i,
        /facebook\.com/i,
        /instagram\.com/i,
        /youtube\.com/i,
        /discord/i
      ];

      const allLinks = socialSection.querySelectorAll('a[href]');
      const externalSocialLinks = Array.from(allLinks).filter(link => {
        const href = link.getAttribute('href');
        return socialPatterns.some(pattern => pattern.test(href));
      });

      // All external social links should have target="_blank"
      externalSocialLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
      });

      // Verify we tested at least 2 links
      expect(externalSocialLinks.length).toBeGreaterThanOrEqual(2);
    });

    test('External social links have rel="noopener noreferrer" for security', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      const socialSection = footer.querySelector('[data-testid="footer-social"], .footer-social, .social-links');

      expect(socialSection).toBeInTheDocument();

      const externalLinks = socialSection.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        // Should have noopener for security
        expect(rel).toBeTruthy();
        expect(rel.includes('noopener')).toBe(true);
      });
    });
  });

  // Additional structural tests
  describe('Footer Multi-Column Layout', () => {
    test('Footer has multi-column layout structure', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      expect(footer).toBeInTheDocument();

      const footerContent = footer.querySelector('.footer-content');
      expect(footerContent).toBeInTheDocument();

      // Check for multiple sections indicating columns
      const sections = footerContent.querySelectorAll('.footer-section, .footer-column');
      expect(sections.length).toBeGreaterThanOrEqual(3);
    });

    test('Footer sections have titles', () => {
      const footer = document.querySelector('[data-testid="footer"], footer.footer');
      const footerContent = footer.querySelector('.footer-content');

      expect(footerContent).toBeInTheDocument();

      const sections = footerContent.querySelectorAll('.footer-section, .footer-column');

      // Most sections should have headings
      let sectionsWithTitles = 0;
      sections.forEach(section => {
        const title = section.querySelector('h3, h4, h5, .footer-title');
        if (title) sectionsWithTitles++;
      });

      expect(sectionsWithTitles).toBeGreaterThanOrEqual(2);
    });
  });
});
