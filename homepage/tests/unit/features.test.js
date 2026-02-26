/**
 * Features Section Unit Tests
 * Owner: Scenario 2 - Features Section
 *
 * Tests for:
 * - Features section visibility and accessibility
 * - Implemented features display (tokio, memtable, minor/major compaction)
 * - Planned/roadmap features display (raft)
 * - Feature layout and card structure
 */

const fs = require('fs');
const path = require('path');

describe('Features Section', () => {
    let document;
    let htmlContent;

    beforeAll(() => {
        // Read the HTML file
        const htmlPath = path.join(__dirname, '../../index.html');
        htmlContent = fs.readFileSync(htmlPath, 'utf8');
    });

    beforeEach(() => {
        // Set up the DOM for each test
        document = new DOMParser().parseFromString(htmlContent, 'text/html');
    });

    describe('Section Visibility and Accessibility', () => {
        test('features section exists with correct id', () => {
            const featuresSection = document.getElementById('features');
            expect(featuresSection).not.toBeNull();
            expect(featuresSection.tagName.toLowerCase()).toBe('section');
        });

        test('features section has proper class for styling', () => {
            const featuresSection = document.getElementById('features');
            expect(featuresSection.classList.contains('features-section')).toBe(true);
        });

        test('navigation link to features section exists', () => {
            const navLink = document.querySelector('a[href="#features"]');
            expect(navLink).not.toBeNull();
            expect(navLink.textContent.toLowerCase()).toContain('feature');
        });

        test('features section has a heading', () => {
            const featuresSection = document.getElementById('features');
            const heading = featuresSection.querySelector('h2');
            expect(heading).not.toBeNull();
            expect(heading.textContent.toLowerCase()).toContain('feature');
        });
    });

    describe('Tokio Feature', () => {
        test('tokio feature card exists', () => {
            const tokioCard = document.querySelector('[data-feature="tokio"]');
            expect(tokioCard).not.toBeNull();
        });

        test('tokio feature has title mentioning tokio and memcached protocol', () => {
            const tokioCard = document.querySelector('[data-feature="tokio"]');
            const title = tokioCard.querySelector('.feature-title');
            expect(title).not.toBeNull();
            const titleText = title.textContent.toLowerCase();
            expect(titleText).toContain('tokio');
            expect(titleText).toContain('memcached');
        });

        test('tokio feature has description', () => {
            const tokioCard = document.querySelector('[data-feature="tokio"]');
            const description = tokioCard.querySelector('.feature-description');
            expect(description).not.toBeNull();
            expect(description.textContent.length).toBeGreaterThan(20);
        });

        test('tokio feature shows implemented status', () => {
            const tokioCard = document.querySelector('[data-feature="tokio"]');
            const status = tokioCard.querySelector('.feature-status');
            expect(status).not.toBeNull();
            expect(status.textContent.toLowerCase()).toContain('implemented');
        });
    });

    describe('Memtable Feature', () => {
        test('memtable feature card exists', () => {
            const memtableCard = document.querySelector('[data-feature="memtable"]');
            expect(memtableCard).not.toBeNull();
        });

        test('memtable feature has title mentioning memtable and skip-list', () => {
            const memtableCard = document.querySelector('[data-feature="memtable"]');
            const title = memtableCard.querySelector('.feature-title');
            expect(title).not.toBeNull();
            const titleText = title.textContent.toLowerCase();
            expect(titleText).toContain('memtable');
            expect(titleText).toContain('skip');
        });

        test('memtable feature has description', () => {
            const memtableCard = document.querySelector('[data-feature="memtable"]');
            const description = memtableCard.querySelector('.feature-description');
            expect(description).not.toBeNull();
            expect(description.textContent.length).toBeGreaterThan(20);
        });

        test('memtable feature shows implemented status', () => {
            const memtableCard = document.querySelector('[data-feature="memtable"]');
            const status = memtableCard.querySelector('.feature-status');
            expect(status).not.toBeNull();
            expect(status.textContent.toLowerCase()).toContain('implemented');
        });
    });

    describe('Minor Compaction Feature', () => {
        test('minor compaction feature card exists', () => {
            const minorCard = document.querySelector('[data-feature="minor-compaction"]');
            expect(minorCard).not.toBeNull();
        });

        test('minor compaction feature has title mentioning minor compaction', () => {
            const minorCard = document.querySelector('[data-feature="minor-compaction"]');
            const title = minorCard.querySelector('.feature-title');
            expect(title).not.toBeNull();
            const titleText = title.textContent.toLowerCase();
            expect(titleText).toContain('minor');
            expect(titleText).toContain('compaction');
        });

        test('minor compaction feature has description', () => {
            const minorCard = document.querySelector('[data-feature="minor-compaction"]');
            const description = minorCard.querySelector('.feature-description');
            expect(description).not.toBeNull();
            expect(description.textContent.length).toBeGreaterThan(20);
        });

        test('minor compaction feature shows implemented status', () => {
            const minorCard = document.querySelector('[data-feature="minor-compaction"]');
            const status = minorCard.querySelector('.feature-status');
            expect(status).not.toBeNull();
            expect(status.textContent.toLowerCase()).toContain('implemented');
        });
    });

    describe('Major Compaction Feature', () => {
        test('major compaction feature card exists', () => {
            const majorCard = document.querySelector('[data-feature="major-compaction"]');
            expect(majorCard).not.toBeNull();
        });

        test('major compaction feature has title mentioning major compaction', () => {
            const majorCard = document.querySelector('[data-feature="major-compaction"]');
            const title = majorCard.querySelector('.feature-title');
            expect(title).not.toBeNull();
            const titleText = title.textContent.toLowerCase();
            expect(titleText).toContain('major');
            expect(titleText).toContain('compaction');
        });

        test('major compaction feature has description', () => {
            const majorCard = document.querySelector('[data-feature="major-compaction"]');
            const description = majorCard.querySelector('.feature-description');
            expect(description).not.toBeNull();
            expect(description.textContent.length).toBeGreaterThan(20);
        });

        test('major compaction feature shows implemented status', () => {
            const majorCard = document.querySelector('[data-feature="major-compaction"]');
            const status = majorCard.querySelector('.feature-status');
            expect(status).not.toBeNull();
            expect(status.textContent.toLowerCase()).toContain('implemented');
        });
    });

    describe('Roadmap/Planned Features', () => {
        test('roadmap section exists', () => {
            const roadmapSection = document.querySelector('.roadmap-section');
            expect(roadmapSection).not.toBeNull();
        });

        test('raft feature card exists in roadmap', () => {
            const raftCard = document.querySelector('[data-feature="raft"]');
            expect(raftCard).not.toBeNull();
        });

        test('raft feature has title mentioning raft', () => {
            const raftCard = document.querySelector('[data-feature="raft"]');
            const title = raftCard.querySelector('.feature-title');
            expect(title).not.toBeNull();
            expect(title.textContent.toLowerCase()).toContain('raft');
        });

        test('raft feature has description', () => {
            const raftCard = document.querySelector('[data-feature="raft"]');
            const description = raftCard.querySelector('.feature-description');
            expect(description).not.toBeNull();
            expect(description.textContent.length).toBeGreaterThan(20);
        });

        test('raft feature shows planned status (not implemented)', () => {
            const raftCard = document.querySelector('[data-feature="raft"]');
            const status = raftCard.querySelector('.feature-status');
            expect(status).not.toBeNull();
            expect(status.textContent.toLowerCase()).toContain('planned');
            expect(status.textContent.toLowerCase()).not.toContain('implemented');
        });

        test('raft feature card has visual distinction for planned status', () => {
            const raftCard = document.querySelector('[data-feature="raft"]');
            expect(raftCard.classList.contains('feature-card-planned')).toBe(true);
        });

        test('planned status has different styling class than implemented', () => {
            const raftCard = document.querySelector('[data-feature="raft"]');
            const raftStatus = raftCard.querySelector('.feature-status');
            expect(raftStatus.classList.contains('feature-status-planned')).toBe(true);

            const tokioCard = document.querySelector('[data-feature="tokio"]');
            const tokioStatus = tokioCard.querySelector('.feature-status');
            expect(tokioStatus.classList.contains('feature-status-implemented')).toBe(true);
        });
    });

    describe('Feature Layout', () => {
        test('features are displayed in a grid container', () => {
            const featuresGrid = document.querySelector('.features-grid');
            expect(featuresGrid).not.toBeNull();
        });

        test('all four implemented features are in the grid', () => {
            const featuresGrid = document.querySelector('.features-grid');
            const featureCards = featuresGrid.querySelectorAll('.feature-card');
            expect(featureCards.length).toBe(4);
        });

        test('each feature card has icon, title, description, and status', () => {
            const featureCards = document.querySelectorAll('.feature-card');
            featureCards.forEach(card => {
                expect(card.querySelector('.feature-icon')).not.toBeNull();
                expect(card.querySelector('.feature-title')).not.toBeNull();
                expect(card.querySelector('.feature-description')).not.toBeNull();
                expect(card.querySelector('.feature-status')).not.toBeNull();
            });
        });

        test('roadmap features are in separate grid', () => {
            const roadmapGrid = document.querySelector('.roadmap-grid');
            expect(roadmapGrid).not.toBeNull();
            const plannedCards = roadmapGrid.querySelectorAll('.feature-card-planned');
            expect(plannedCards.length).toBeGreaterThanOrEqual(1);
        });
    });
});
