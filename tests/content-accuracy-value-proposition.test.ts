import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

/**
 * Content Accuracy Tests - Value Proposition
 *
 * These tests verify that the homepage content accurately represents
 * MirDB's capabilities and value proposition without overstating features.
 */

describe('Content Accuracy - Value Proposition', () => {
  let mainDocument: Document;
  let websiteDocument: Document;
  let readmeContent: string;
  let configContent: string;

  beforeEach(() => {
    // Load main index.html
    const mainHtmlPath = path.resolve(__dirname, '../index.html');
    const mainHtml = fs.readFileSync(mainHtmlPath, 'utf-8');
    const mainDom = new JSDOM(mainHtml);
    mainDocument = mainDom.window.document;

    // Load website/index.html
    const websiteHtmlPath = path.resolve(__dirname, '../website/index.html');
    const websiteHtml = fs.readFileSync(websiteHtmlPath, 'utf-8');
    const websiteDom = new JSDOM(websiteHtml);
    websiteDocument = websiteDom.window.document;

    // Load README.md for project status verification
    const readmePath = path.resolve(__dirname, '../README.md');
    readmeContent = fs.readFileSync(readmePath, 'utf-8');

    // Load configuration file
    const configPath = path.resolve(__dirname, '../etc/mirdb.toml');
    configContent = fs.readFileSync(configPath, 'utf-8');
  });

  describe('TC1: Memcached Compatibility Claim Accuracy', () => {
    it('should accurately describe memcached text protocol support in main homepage', () => {
      const featuresSection = mainDocument.querySelector('#features');
      const featuresText = featuresSection?.textContent || '';

      // Should mention memcached protocol/compatibility
      expect(featuresText.toLowerCase()).toMatch(/memcached.*protocol|memcached.*compatib/i);
    });

    it('should accurately describe memcached text protocol support in website homepage', () => {
      const featuresSection = websiteDocument.querySelector('#features, .features-section');
      const featuresText = featuresSection?.textContent || '';

      // Should mention memcached protocol/compatibility
      expect(featuresText.toLowerCase()).toMatch(/memcached.*protocol|memcached.*compatib/i);
    });

    it('should not overstate memcached compatibility - should mention text protocol', () => {
      // The claim should be about memcached protocol compatibility, not full memcached replacement
      const mainText = mainDocument.body.textContent || '';
      const websiteText = websiteDocument.body.textContent || '';

      // Should mention memcached clients can work with it
      expect(mainText.toLowerCase()).toMatch(/memcached.*client|client.*memcached/i);
      expect(websiteText.toLowerCase()).toMatch(/memcached.*client|client.*memcached/i);
    });

    it('should describe accurate command support', () => {
      const commandsSection = mainDocument.querySelector('#commands, .commands-section');
      const commandsText = commandsSection?.textContent || '';

      // Verify supported commands are listed accurately
      expect(commandsText).toContain('SET');
      expect(commandsText).toContain('GET');
      expect(commandsText).toContain('DELETE');
      expect(commandsText).toContain('ADD');
      expect(commandsText).toContain('REPLACE');
    });
  });

  describe('TC2: Persistence/SSTable Claim Accuracy', () => {
    it('should accurately describe SSTable-based persistence in main homepage', () => {
      const featuresSection = mainDocument.querySelector('#features');
      const featuresText = featuresSection?.textContent || '';

      // Should mention SSTable-based persistence
      expect(featuresText.toLowerCase()).toMatch(/sstable|sst|persistent.*storage/i);
    });

    it('should accurately describe SSTable-based persistence in website homepage', () => {
      const featuresSection = websiteDocument.querySelector('#features, .features-section');
      const featuresText = featuresSection?.textContent || '';

      // Should mention SSTable-based persistence
      expect(featuresText.toLowerCase()).toMatch(/sstable|sst|persistent.*storage/i);
    });

    it('should describe data durability accurately', () => {
      const mainText = mainDocument.body.textContent || '';

      // Should mention data survives restarts
      expect(mainText.toLowerCase()).toMatch(/survives.*restart|persist|durable|durability/i);
    });

    it('should describe LSM tree architecture accurately', () => {
      const featuresSection = mainDocument.querySelector('#features');
      const featuresText = featuresSection?.textContent || '';

      // Should mention LSM tree architecture
      expect(featuresText.toLowerCase()).toMatch(/lsm.*tree|log.*structured.*merge/i);
    });

    it('should describe write-optimized architecture', () => {
      const architectureSection = mainDocument.querySelector('#architecture, .architecture-section');
      const archText = architectureSection?.textContent || '';

      // Should describe write path with WAL -> Memtable -> SSTable flow
      expect(archText.toLowerCase()).toMatch(/wal|write.*ahead.*log/i);
      expect(archText.toLowerCase()).toMatch(/memtable/i);
    });
  });

  describe('TC3: Raft Consensus - Future Feature Marking', () => {
    it('should NOT claim Raft consensus is implemented in main homepage', () => {
      const mainText = mainDocument.body.textContent || '';

      // Raft should NOT be mentioned as an implemented feature
      // If mentioned, it should be clearly marked as planned/future
      const raftMatch = mainText.toLowerCase().match(/raft/i);
      expect(raftMatch).toBeNull(); // Should not be in homepage
    });

    it('should NOT claim Raft consensus is implemented in website homepage', () => {
      const websiteText = websiteDocument.body.textContent || '';

      // Raft should NOT be mentioned as an implemented feature
      const raftMatch = websiteText.toLowerCase().match(/raft/i);
      expect(raftMatch).toBeNull(); // Should not be in homepage
    });

    it('should mark Raft as planned/TODO in README', () => {
      // README should show Raft as unchecked TODO
      // Format: * [ ] raft
      expect(readmeContent).toMatch(/\[\s*\]\s*raft/i);
    });

    it('should NOT claim distributed operation is available', () => {
      const mainText = mainDocument.body.textContent || '';
      const websiteText = websiteDocument.body.textContent || '';

      // Should not claim distributed features without Raft
      expect(mainText.toLowerCase()).not.toMatch(/distributed.*consensus|consensus.*algorithm/i);
      expect(websiteText.toLowerCase()).not.toMatch(/distributed.*consensus|consensus.*algorithm/i);
    });
  });

  describe('TC4: Default Port Accuracy (12333)', () => {
    it('should show default port 12333 in main homepage', () => {
      const gettingStartedSection = mainDocument.querySelector('#getting-started');
      const sectionText = gettingStartedSection?.textContent || '';

      expect(sectionText).toContain('12333');
    });

    it('should show default port 12333 in website homepage', () => {
      const gettingStartedSection = websiteDocument.querySelector('#getting-started');
      const sectionText = gettingStartedSection?.textContent || '';

      expect(sectionText).toContain('12333');
    });

    it('should match default port in actual configuration file', () => {
      // Verify config file has correct default port
      expect(configContent).toMatch(/addr\s*=\s*"[^"]*12333/);
    });

    it('should use port 12333 in code examples', () => {
      // Check Python example uses correct port
      const codeExamples = mainDocument.querySelectorAll('pre code');
      const hasCorrectPort = Array.from(codeExamples).some(code =>
        code.textContent?.includes('12333')
      );
      expect(hasCorrectPort).toBe(true);
    });

    it('should show port 12333 in configuration section', () => {
      const configSection = websiteDocument.querySelector('#configuration, .configuration-section');
      const configText = configSection?.textContent || '';

      expect(configText).toContain('12333');
    });
  });

  describe('Technical Accuracy - No Overstating Features', () => {
    it('should list only implemented features in features section', () => {
      const featuresSection = mainDocument.querySelector('#features');
      const featuresText = featuresSection?.textContent?.toLowerCase() || '';

      // Implemented features should be present
      expect(featuresText).toMatch(/memcached/);
      expect(featuresText).toMatch(/persist|storage/);
      expect(featuresText).toMatch(/lsm|tree/);
      expect(featuresText).toMatch(/rust|tokio/);

      // Unimplemented features should NOT be claimed
      expect(featuresText).not.toMatch(/raft/);
      expect(featuresText).not.toMatch(/distributed.*consensus/);
    });

    it('should accurately describe project status', () => {
      // README shows what is implemented vs planned
      expect(readmeContent).toMatch(/\[x\]\s*tokio.*memcached/i);
      expect(readmeContent).toMatch(/\[x\]\s*memtable.*skiplist/i);
      expect(readmeContent).toMatch(/\[x\]\s*minor\s*compaction/i);
      expect(readmeContent).toMatch(/\[x\]\s*major\s*compaction/i);
      expect(readmeContent).toMatch(/\[\s*\]\s*raft/i); // Not implemented
    });

    it('should not claim features that are not implemented', () => {
      const mainText = mainDocument.body.textContent?.toLowerCase() || '';

      // Should not claim unimplemented features
      expect(mainText).not.toMatch(/replication|replica/i);
      expect(mainText).not.toMatch(/cluster|clustering/i);
      expect(mainText).not.toMatch(/distributed|distributed.*storage/i);
    });
  });

  describe('Configuration Values Accuracy', () => {
    it('should show accurate default memtable size (4MB)', () => {
      const configSection = websiteDocument.querySelector('#configuration');
      const configText = configSection?.textContent || '';

      expect(configText).toMatch(/4\s*MB|4M/i);
      expect(configContent).toMatch(/mem_table_max_size\s*=\s*"4M"/);
    });

    it('should show accurate default SSTable size (100MB)', () => {
      const configSection = websiteDocument.querySelector('#configuration');
      const configText = configSection?.textContent || '';

      expect(configText).toMatch(/100\s*MB|100M/i);
      expect(configContent).toMatch(/sst_max_size\s*=\s*"100M"/);
    });

    it('should show accurate max LSM levels (7)', () => {
      const configSection = websiteDocument.querySelector('#configuration');
      const configText = configSection?.textContent || '';

      expect(configText).toContain('7');
      expect(configContent).toMatch(/max_level\s*=\s*7/);
    });
  });
});
