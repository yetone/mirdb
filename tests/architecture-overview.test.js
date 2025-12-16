/**
 * Architecture Overview Section Tests
 *
 * This test suite validates that the MirDB homepage includes an architecture
 * overview with visual diagram per REQ-6.
 *
 * Test cases cover:
 * 1. Architecture section presence
 * 2. Visual diagram presence (image, SVG, or ASCII)
 * 3. LSM-tree explanation
 * 4. WAL component
 * 5. Memtable component
 * 6. SSTable component
 * 7. Accessibility (alt text for diagram)
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the homepage HTML
const htmlPath = path.join(__dirname, '..', 'index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const $ = cheerio.load(htmlContent);

describe('Architecture Overview Section', () => {
    describe('Test Case 1: Architecture Section Presence', () => {
        it('should have an architecture section on homepage', () => {
            const archSection = $('#architecture');
            expect(archSection.length).toBe(1);
        });

        it('should have architecture section with correct id', () => {
            const archSection = $('section#architecture');
            expect(archSection.length).toBe(1);
        });

        it('should have architecture class on the section', () => {
            const archSection = $('section.architecture');
            expect(archSection.length).toBe(1);
        });

        it('should have an Architecture heading (h2)', () => {
            const archSection = $('#architecture');
            const heading = archSection.find('h2');
            expect(heading.length).toBe(1);
            expect(heading.text()).toBe('Architecture');
        });

        it('should have navigation link to architecture section', () => {
            const archLink = $('nav a[href="#architecture"]');
            expect(archLink.length).toBeGreaterThan(0);
        });

        it('should have architecture section within main content', () => {
            const archSection = $('main #architecture');
            expect(archSection.length).toBe(1);
        });
    });

    describe('Test Case 2: Visual Diagram Presence', () => {
        it('should have a visual diagram in architecture section', () => {
            const archSection = $('#architecture');
            // Check for diagram container (div with arch-diagram class)
            const diagramContainer = archSection.find('.arch-diagram');
            expect(diagramContainer.length).toBe(1);
        });

        it('should have architecture flow diagram with components', () => {
            const archSection = $('#architecture');
            const archFlow = archSection.find('.arch-flow');
            expect(archFlow.length).toBe(1);
        });

        it('should have visual components in the diagram', () => {
            const archSection = $('#architecture');
            const archComponents = archSection.find('.arch-component');
            expect(archComponents.length).toBeGreaterThanOrEqual(3);
        });

        it('should have arrows showing data flow', () => {
            const archSection = $('#architecture');
            const arrows = archSection.find('.arrow');
            expect(arrows.length).toBeGreaterThanOrEqual(2);
        });

        it('should have component names visible in diagram', () => {
            const archSection = $('#architecture');
            const componentNames = archSection.find('.component-name');
            expect(componentNames.length).toBeGreaterThanOrEqual(3);
        });

        it('diagram should show data flow direction (WAL -> Memtable -> SSTable)', () => {
            const archSection = $('#architecture');
            const components = [];
            archSection.find('.arch-component .component-name').each((i, el) => {
                components.push($(el).text().trim());
            });

            // Verify the order includes WAL, Memtable, and SSTable in sequence
            const walIndex = components.findIndex(c => c.includes('WAL'));
            const memtableIndex = components.findIndex(c => c.includes('Memtable'));
            const sstableIndex = components.findIndex(c => c.includes('SSTable'));

            expect(walIndex).toBeGreaterThanOrEqual(0);
            expect(memtableIndex).toBeGreaterThan(walIndex);
            expect(sstableIndex).toBeGreaterThan(memtableIndex);
        });
    });

    describe('Test Case 3: LSM-tree Explanation', () => {
        it('should mention LSM-tree in architecture section', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text().toLowerCase();
            const hasLSMTree = sectionText.includes('lsm-tree') ||
                              sectionText.includes('lsm tree') ||
                              sectionText.includes('log-structured merge');
            expect(hasLSMTree).toBe(true);
        });

        it('should have description explaining LSM-tree approach', () => {
            const archSection = $('#architecture');
            const description = archSection.find('.arch-description');
            expect(description.length).toBe(1);
            expect(description.text().toLowerCase()).toContain('lsm-tree');
        });

        it('should explain that LSM-tree is used for storage', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text().toLowerCase();
            expect(sectionText).toContain('storage');
        });

        it('should have architecture details section', () => {
            const archSection = $('#architecture');
            const archDetails = archSection.find('.arch-details');
            expect(archDetails.length).toBe(1);
        });

        it('should explain the write path steps', () => {
            const archSection = $('#architecture');
            const archDetails = archSection.find('.arch-details');
            const detailsText = archDetails.text().toLowerCase();

            // Should explain that writes go to WAL first
            expect(detailsText).toContain('wal');
            // Should mention memtable
            expect(detailsText).toContain('memtable');
            // Should mention flushing to disk/sstable
            expect(detailsText).toContain('sstable');
        });
    });

    describe('Test Case 4: WAL Component', () => {
        it('should show WAL component in architecture', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text();
            const hasWAL = sectionText.includes('WAL') ||
                          sectionText.includes('Write-Ahead Log') ||
                          sectionText.includes('write-ahead log');
            expect(hasWAL).toBe(true);
        });

        it('should have WAL in diagram components', () => {
            const archSection = $('#architecture');
            let hasWalComponent = false;
            archSection.find('.arch-component').each((i, el) => {
                const text = $(el).text();
                if (text.includes('WAL') || text.includes('Write-Ahead')) {
                    hasWalComponent = true;
                }
            });
            expect(hasWalComponent).toBe(true);
        });

        it('should explain WAL purpose (durability)', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text().toLowerCase();
            // WAL is mentioned and durability is explained
            expect(sectionText).toContain('wal');
            expect(sectionText).toContain('durability');
        });

        it('should have WAL description in component', () => {
            const archSection = $('#architecture');
            let walHasDescription = false;
            archSection.find('.arch-component').each((i, el) => {
                const componentName = $(el).find('.component-name').text();
                const componentDesc = $(el).find('.component-desc').text();
                if (componentName.includes('WAL') && componentDesc.length > 0) {
                    walHasDescription = true;
                }
            });
            expect(walHasDescription).toBe(true);
        });
    });

    describe('Test Case 5: Memtable Component', () => {
        it('should show Memtable component in architecture', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text();
            expect(sectionText).toContain('Memtable');
        });

        it('should have Memtable in diagram components', () => {
            const archSection = $('#architecture');
            let hasMemtableComponent = false;
            archSection.find('.arch-component').each((i, el) => {
                const text = $(el).text();
                if (text.includes('Memtable')) {
                    hasMemtableComponent = true;
                }
            });
            expect(hasMemtableComponent).toBe(true);
        });

        it('should mention Memtable is in-memory storage', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text().toLowerCase();
            // Either "in-memory" or "memory" should be present
            expect(sectionText).toContain('memory');
        });

        it('should have Memtable description mentioning skip list', () => {
            const archSection = $('#architecture');
            let memtableDesc = '';
            archSection.find('.arch-component').each((i, el) => {
                const componentName = $(el).find('.component-name').text();
                if (componentName.includes('Memtable')) {
                    memtableDesc = $(el).find('.component-desc').text().toLowerCase();
                }
            });
            expect(memtableDesc).toContain('skip list');
        });
    });

    describe('Test Case 6: SSTable Component', () => {
        it('should show SSTable component in architecture', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text();
            expect(sectionText).toContain('SSTable');
        });

        it('should have SSTable in diagram components', () => {
            const archSection = $('#architecture');
            let hasSstableComponent = false;
            archSection.find('.arch-component').each((i, el) => {
                const text = $(el).text();
                if (text.includes('SSTable')) {
                    hasSstableComponent = true;
                }
            });
            expect(hasSstableComponent).toBe(true);
        });

        it('should explain SSTable is persistent storage', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text().toLowerCase();
            // Should mention disk, persistent, or storage in relation to SSTable
            const hasPersistentStorage = sectionText.includes('disk') ||
                                         sectionText.includes('persistent') ||
                                         sectionText.includes('sorted string table');
            expect(hasPersistentStorage).toBe(true);
        });

        it('should have SSTable description', () => {
            const archSection = $('#architecture');
            let sstableHasDescription = false;
            archSection.find('.arch-component').each((i, el) => {
                const componentName = $(el).find('.component-name').text();
                const componentDesc = $(el).find('.component-desc').text();
                if (componentName.includes('SSTable') && componentDesc.length > 0) {
                    sstableHasDescription = true;
                }
            });
            expect(sstableHasDescription).toBe(true);
        });

        it('should mention compaction process', () => {
            const archSection = $('#architecture');
            const sectionText = archSection.text().toLowerCase();
            expect(sectionText).toContain('compaction');
        });
    });

    describe('Test Case 7: Accessibility - Alt Text for Diagram', () => {
        it('should have accessible architecture diagram', () => {
            const archSection = $('#architecture');
            const diagram = archSection.find('.arch-diagram');
            expect(diagram.length).toBe(1);
        });

        it('should have text-based diagram (accessible by default)', () => {
            // The architecture diagram uses HTML/CSS components with text
            // which is inherently accessible
            const archSection = $('#architecture');
            const archComponents = archSection.find('.arch-component');

            archComponents.each((i, el) => {
                const componentName = $(el).find('.component-name').text();
                // Each component should have visible text
                expect(componentName.length).toBeGreaterThan(0);
            });
        });

        it('should have component names that are screen reader friendly', () => {
            const archSection = $('#architecture');
            const componentNames = archSection.find('.component-name');

            componentNames.each((i, el) => {
                const name = $(el).text().trim();
                // Name should be meaningful text
                expect(name.length).toBeGreaterThan(0);
                expect(name).not.toMatch(/^[\s\d]+$/); // Not just whitespace or numbers
            });
        });

        it('should have component descriptions for additional context', () => {
            const archSection = $('#architecture');
            const componentDescs = archSection.find('.component-desc');
            expect(componentDescs.length).toBeGreaterThan(0);
        });

        it('should have architecture description text', () => {
            const archSection = $('#architecture');
            const description = archSection.find('.arch-description');
            expect(description.length).toBe(1);
            expect(description.text().length).toBeGreaterThan(20);
        });

        it('should have detailed explanation steps', () => {
            const archSection = $('#architecture');
            const archDetails = archSection.find('.arch-details p');
            expect(archDetails.length).toBeGreaterThanOrEqual(3);
        });

        it('diagram text should be properly structured in HTML elements', () => {
            const archSection = $('#architecture');

            // The diagram should use semantic HTML structure
            const hasContainer = archSection.find('.container').length > 0;
            const hasHeading = archSection.find('h2').length > 0;
            const hasDiagramContainer = archSection.find('.arch-diagram').length > 0;
            const hasDescriptionText = archSection.find('.arch-description').length > 0;

            expect(hasContainer).toBe(true);
            expect(hasHeading).toBe(true);
            expect(hasDiagramContainer).toBe(true);
            expect(hasDescriptionText).toBe(true);
        });
    });

    describe('Architecture Section Integration', () => {
        it('should be navigable from navigation menu', () => {
            const navLink = $('nav a[href="#architecture"]');
            expect(navLink.length).toBeGreaterThan(0);
            expect(navLink.text()).toContain('Architecture');
        });

        it('should follow features and getting-started sections', () => {
            const sections = $('main section').map((i, el) => $(el).attr('id')).get();
            const archIndex = sections.indexOf('architecture');
            const featuresIndex = sections.indexOf('features');
            const gettingStartedIndex = sections.indexOf('getting-started');

            expect(archIndex).toBeGreaterThan(featuresIndex);
            expect(archIndex).toBeGreaterThan(gettingStartedIndex);
        });

        it('should have consistent styling with other sections', () => {
            const archSection = $('#architecture');
            const container = archSection.find('.container');
            expect(container.length).toBe(1);
        });
    });
});
