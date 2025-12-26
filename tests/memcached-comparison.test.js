/**
 * Memcached Comparison Section Tests
 *
 * Tests for verifying comparison with standard memcached is available (REQ-9, US-5)
 *
 * This test suite validates:
 * - Content explaining differences from standard memcached exists
 * - Text explaining data survives restarts (unlike memcached) is present
 * - Commands SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND are mentioned
 */

describe('Memcached Comparison Section', () => {
    let comparisonSection;
    let documentBody;

    beforeAll(() => {
        documentBody = document.body;
        // Look for comparison section - could be section with id="comparison" or class="comparison"
        comparisonSection = document.querySelector('#comparison') ||
            document.querySelector('.comparison') ||
            document.querySelector('[data-section="comparison"]');
    });

    describe('Test Case 1: Content explaining differences from standard memcached exists', () => {
        test('comparison section should exist in the document', () => {
            expect(comparisonSection).not.toBeNull();
        });

        test('comparison section should have a heading', () => {
            const heading = comparisonSection.querySelector('h2, h3');
            expect(heading).not.toBeNull();
        });

        test('comparison section should mention "memcached" in heading or content', () => {
            const sectionText = comparisonSection.textContent.toLowerCase();
            expect(sectionText).toContain('memcached');
        });

        test('comparison section should highlight differences or comparison', () => {
            const sectionText = comparisonSection.textContent.toLowerCase();
            const hasDifferentiatorContent = sectionText.includes('differ') ||
                sectionText.includes('compar') ||
                sectionText.includes('vs') ||
                sectionText.includes('unlike') ||
                sectionText.includes('advantage') ||
                sectionText.includes('benefit');
            expect(hasDifferentiatorContent).toBe(true);
        });

        test('comparison section should be within the main content area', () => {
            const main = document.querySelector('main');
            expect(main).not.toBeNull();
            expect(main.contains(comparisonSection)).toBe(true);
        });
    });

    describe('Test Case 2: Text explaining data survives restarts (unlike memcached) is present', () => {
        test('comparison section should mention persistence', () => {
            const sectionText = comparisonSection.textContent.toLowerCase();
            const hasPersistenceContent = sectionText.includes('persist') ||
                sectionText.includes('durable') ||
                sectionText.includes('durability') ||
                sectionText.includes('disk');
            expect(hasPersistenceContent).toBe(true);
        });

        test('comparison section should explain data survival through restarts', () => {
            const sectionText = comparisonSection.textContent.toLowerCase();
            const hasRestartContent = sectionText.includes('restart') ||
                sectionText.includes('surviv') ||
                sectionText.includes('recover') ||
                sectionText.includes('lost') ||
                sectionText.includes('retained') ||
                sectionText.includes('preserv');
            expect(hasRestartContent).toBe(true);
        });

        test('comparison should contrast with memcached in-memory limitation', () => {
            const sectionText = comparisonSection.textContent.toLowerCase();
            // Should mention that traditional memcached loses data or is memory-only
            const hasContrastContent = sectionText.includes('unlike memcached') ||
                sectionText.includes('standard memcached') ||
                sectionText.includes('traditional memcached') ||
                sectionText.includes('memcached loses') ||
                sectionText.includes('memcached is') ||
                sectionText.includes('in-memory') ||
                sectionText.includes('memory-only') ||
                sectionText.includes('volatile');
            expect(hasContrastContent).toBe(true);
        });

        test('comparison section should mention SSTables or LSM tree for persistence', () => {
            const sectionText = comparisonSection.textContent.toLowerCase();
            const hasStorageContent = sectionText.includes('sstable') ||
                sectionText.includes('lsm') ||
                sectionText.includes('log-structured') ||
                sectionText.includes('write-ahead') ||
                sectionText.includes('disk storage');
            expect(hasStorageContent).toBe(true);
        });
    });

    describe('Test Case 3: Commands SET, GET, DELETE, ADD, REPLACE, APPEND, PREPEND are mentioned', () => {
        let pageText;

        beforeAll(() => {
            // Get full page text to check for commands
            pageText = documentBody.textContent;
        });

        test('SET command should be documented', () => {
            expect(pageText).toContain('SET');
        });

        test('GET command should be documented', () => {
            expect(pageText).toContain('GET');
        });

        test('DELETE command should be documented', () => {
            expect(pageText).toContain('DELETE');
        });

        test('ADD command should be documented', () => {
            expect(pageText).toContain('ADD');
        });

        test('REPLACE command should be documented', () => {
            expect(pageText).toContain('REPLACE');
        });

        test('APPEND command should be documented', () => {
            expect(pageText).toContain('APPEND');
        });

        test('PREPEND command should be documented', () => {
            expect(pageText).toContain('PREPEND');
        });

        test('commands should be presented in a structured format (list or table)', () => {
            // Check for a commands section or list structure
            const commandsSection = document.querySelector('#commands') ||
                document.querySelector('.commands') ||
                document.querySelector('[data-section="commands"]');

            if (commandsSection) {
                // If dedicated commands section exists, verify it has list items
                const listItems = commandsSection.querySelectorAll('li');
                expect(listItems.length).toBeGreaterThan(0);
            } else {
                // Otherwise check comparison section has command references
                const comparisonText = comparisonSection.textContent;
                const hasMultipleCommands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND']
                    .filter(cmd => comparisonText.includes(cmd)).length >= 4;
                expect(hasMultipleCommands).toBe(true);
            }
        });

        test('all required memcached commands should be mentioned somewhere on page', () => {
            const requiredCommands = ['SET', 'GET', 'DELETE', 'ADD', 'REPLACE', 'APPEND', 'PREPEND'];
            const missingCommands = requiredCommands.filter(cmd => !pageText.includes(cmd));
            expect(missingCommands).toEqual([]);
        });
    });
});
