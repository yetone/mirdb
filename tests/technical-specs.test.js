/**
 * Technical Specifications Section Tests
 *
 * Tests for verifying configuration defaults and system requirements
 * are displayed correctly (REQ-8)
 */

describe('Technical Specifications Section', () => {
    let techSpecsSection;

    beforeAll(() => {
        techSpecsSection = document.querySelector('#technical-specs') || document.querySelector('.technical-specs');
    });

    describe('Test Case 1: Default port configuration (12333) is present', () => {
        test('technical specs section should exist', () => {
            expect(techSpecsSection).not.toBeNull();
        });

        test('section should contain text "12333" (default port)', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('12333');
        });

        test('port should be displayed in configuration defaults table', () => {
            const table = techSpecsSection.querySelector('.specs-table');
            expect(table).not.toBeNull();
            expect(table.textContent).toContain('12333');
        });

        test('listen address should show 0.0.0.0:12333', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('0.0.0.0:12333');
        });
    });

    describe('Test Case 2: Configuration defaults section with memtable/SSTable sizes', () => {
        test('configuration defaults card should exist', () => {
            const configCard = techSpecsSection.querySelector('.specs-card');
            expect(configCard).not.toBeNull();
        });

        test('should display memtable max size (4MB)', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('4MB');
        });

        test('should display SSTable max size (100MB)', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('100MB');
        });

        test('should display block size (4KB)', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('4KB');
        });

        test('should display max LSM levels (7)', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toMatch(/7/);
        });

        test('should display work directory (/tmp/mirdb)', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('/tmp/mirdb');
        });

        test('configuration table should have proper structure', () => {
            const table = techSpecsSection.querySelector('.specs-table');
            expect(table).not.toBeNull();

            const headerRow = table.querySelector('thead tr');
            expect(headerRow).not.toBeNull();

            const dataRows = table.querySelectorAll('tbody tr');
            expect(dataRows.length).toBeGreaterThan(0);
        });
    });

    describe('Test Case 3: Rust/Tokio requirement is mentioned', () => {
        test('should mention Rust', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('Rust');
        });

        test('should mention Tokio', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText).toContain('Tokio');
        });

        test('system requirements card should exist', () => {
            const cards = techSpecsSection.querySelectorAll('.specs-card');
            expect(cards.length).toBeGreaterThanOrEqual(2);
        });

        test('requirements list should contain multiple items', () => {
            const requirementsList = techSpecsSection.querySelector('.requirements-list');
            expect(requirementsList).not.toBeNull();

            const items = requirementsList.querySelectorAll('li');
            expect(items.length).toBeGreaterThanOrEqual(3);
        });

        test('should mention operating system requirements', () => {
            const sectionText = techSpecsSection.textContent;
            expect(sectionText.toLowerCase()).toMatch(/linux|macos|windows|operating system/i);
        });
    });

    describe('Navigation to Technical Specs', () => {
        test('navigation should include link to technical specs section', () => {
            const navLinks = document.querySelectorAll('.nav-links a');
            const techSpecsLink = Array.from(navLinks).find(link =>
                link.getAttribute('href') === '#technical-specs'
            );
            expect(techSpecsLink).not.toBeNull();
        });

        test('section should have proper id for navigation', () => {
            const section = document.querySelector('#technical-specs');
            expect(section).not.toBeNull();
        });
    });

    describe('Section Structure and Accessibility', () => {
        test('section should have h2 heading', () => {
            const h2 = techSpecsSection.querySelector('h2');
            expect(h2).not.toBeNull();
            expect(h2.textContent).toContain('Technical Specifications');
        });

        test('section should have description paragraph', () => {
            const description = techSpecsSection.querySelector('.section-description');
            expect(description).not.toBeNull();
        });

        test('section should use semantic section element', () => {
            expect(techSpecsSection.tagName.toLowerCase()).toBe('section');
        });

        test('tables should have proper headers for accessibility', () => {
            const table = techSpecsSection.querySelector('.specs-table');
            if (table) {
                const thead = table.querySelector('thead');
                expect(thead).not.toBeNull();

                const th = thead.querySelectorAll('th');
                expect(th.length).toBeGreaterThan(0);
            }
        });
    });
});
