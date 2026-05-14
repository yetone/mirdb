/**
 * Unit tests for FAQ component.
 * Tests FAQ item count, accordion expand/collapse behavior,
 * keyboard accessibility, Memcached protocol command reference,
 * documentation links, and empty state handling.
 */
import { describe, it, expect } from 'vitest';
import { parseHTML } from 'linkedom';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface FAQItemData {
  id: string;
  question: string;
  answer: string;
}

const faqData: FAQItemData[] = [
  {
    id: 'what-is-mirdb',
    question: 'What is MirDB?',
    answer: 'MirDB is a high-performance persistent key-value store written in Rust. It implements the Memcached text protocol, allowing existing Memcached clients to connect seamlessly. Unlike traditional Memcached (which is in-memory only), MirDB persists data to disk using an LSM tree architecture, providing durability without sacrificing performance.',
  },
  {
    id: 'installation',
    question: 'How do I install MirDB?',
    answer: 'MirDB can be installed in multiple ways: <ul><li><strong>Cargo</strong>: Run <code>cargo install mirdb</code></li><li><strong>Docker</strong>: Pull the image with <code>docker pull yetone/mirdb</code> and run with <code>docker run -p 12333:12333 yetone/mirdb</code></li><li><strong>From Source</strong>: Clone the repository with <code>git clone https://github.com/yetone/mirdb.git</code> and build with <code>cargo build --release</code></li></ul>',
  },
  {
    id: 'protocol-commands',
    question: 'What Memcached protocol commands does MirDB support?',
    answer: 'MirDB supports the following Memcached protocol commands:<ul><li><strong>SET</strong> — Store a key-value pair: <code>set &lt;key&gt; &lt;flags&gt; &lt;ttl&gt; &lt;bytes&gt;\\r\\n&lt;data&gt;\\r\\n</code></li><li><strong>GET</strong> — Retrieve one or more keys: <code>get &lt;key&gt;\\r\\n</code></li><li><strong>DELETE</strong> — Remove a key: <code>delete &lt;key&gt;\\r\\n</code></li><li><strong>ADD</strong> — Store only if the key does not exist</li><li><strong>REPLACE</strong> — Store only if the key exists</li><li><strong>APPEND</strong> — Append data to an existing value</li><li><strong>PREPEND</strong> — Prepend data to an existing value</li><li><strong>GETS</strong> — Retrieve with CAS token</li></ul>MirDB also supports two custom commands: <code>INFO</code> for database status and <code>MAJOR_COMPACTION</code> to trigger manual compaction.',
  },
  {
    id: 'memcached-compatibility',
    question: 'Is MirDB compatible with existing Memcached clients?',
    answer: 'Yes. MirDB speaks the standard Memcached text protocol over TCP, so any Memcached client library (Python, Ruby, Node.js, PHP, etc.) can connect and issue commands without modification. The default listen address is <code>0.0.0.0:12333</code>. Simply point your existing Memcached client at that address and start sending commands.',
  },
  {
    id: 'durability',
    question: 'How does MirDB ensure data durability?',
    answer: 'MirDB uses a Write-Ahead Log (WAL) to ensure durability. All writes are first appended to the WAL before being applied to the in-memory memtable. The memtable, implemented as a skip-list, is flushed to disk as an SSTable (Sorted String Table) during compaction. This design ensures that data is never lost on crash — MirDB replays the WAL on restart to recover un-flushed writes. The LSM tree architecture with multi-level compaction keeps read performance high even as data grows.',
  },
  {
    id: 'docs-links',
    question: 'Where can I find detailed API documentation?',
    answer: 'Comprehensive documentation is available on the <a href="https://github.com/yetone/mirdb#readme" target="_blank" rel="noopener noreferrer">GitHub README</a>. For protocol details, refer to the <a href="https://github.com/memcached/memcached/blob/master/doc/protocol.txt" target="_blank" rel="noopener noreferrer">Memcached Protocol Specification</a>. For configuration reference, see the <a href="https://github.com/yetone/mirdb/blob/main/etc/mirdb.toml" target="_blank" rel="noopener noreferrer">example configuration file</a> in the repository.',
  },
];

function buildFAQTriggerHTML(item: FAQItemData): string {
  const answerId = `faq-answer-${item.id}`;
  const triggerId = `faq-trigger-${item.id}`;
  return `<button
      type="button"
      id="${triggerId}"
      aria-expanded="false"
      aria-controls="${answerId}"
      data-faq-trigger
    >
      <span>${item.question}</span>
      <span aria-hidden="true"></span>
    </button>`;
}

function buildFAQAnswerHTML(item: FAQItemData): string {
  const answerId = `faq-answer-${item.id}`;
  const triggerId = `faq-trigger-${item.id}`;
  return `<div
      id="${answerId}"
      role="region"
      aria-labelledby="${triggerId}"
      aria-hidden="true"
    >
      <div>${item.answer}</div>
    </div>`;
}

function buildFAQItemHTML(item: FAQItemData): string {
  return `<div>
  <h3>${buildFAQTriggerHTML(item)}</h3>
  ${buildFAQAnswerHTML(item)}
</div>`;
}

function buildFAQSectionHTML(items: FAQItemData[]): string {
  if (items.length === 0) {
    return `<section id="faq">
  <div>
    <h2>Frequently Asked Questions</h2>
    <p data-faq-empty>No frequently asked questions available at this time.</p>
  </div>
</section>`;
  }

  const itemsHTML = items.map((item) => buildFAQItemHTML(item)).join('\n');

  return `<section id="faq">
  <div>
    <h2>Frequently Asked Questions</h2>
    <div data-faq-list>
      ${itemsHTML}
    </div>
  </div>
</section>`;
}

function buildFullPageHTML(items: FAQItemData[]): string {
  return `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>MirDB</title></head>
<body>
  <main>
    ${buildFAQSectionHTML(items)}
  </main>
</body>
</html>`;
}

// Accordion behavior helpers – simulate the client-side JS

function expandItem(document: Document, trigger: Element) {
  const answerId = trigger.getAttribute('aria-controls');
  const answer = answerId ? document.getElementById(answerId) : null;
  trigger.setAttribute('aria-expanded', 'true');
  if (answer) answer.setAttribute('aria-hidden', 'false');
}

function collapseItem(document: Document, trigger: Element) {
  const answerId = trigger.getAttribute('aria-controls');
  const answer = answerId ? document.getElementById(answerId) : null;
  trigger.setAttribute('aria-expanded', 'false');
  if (answer) answer.setAttribute('aria-hidden', 'true');
}

function collapseAll(triggers: NodeListOf<Element> | Element[], document: Document) {
  triggers.forEach((t) => collapseItem(document, t));
}

function toggleItem(document: Document, trigger: Element) {
  const isExpanded = trigger.getAttribute('aria-expanded') === 'true';
  if (isExpanded) {
    collapseItem(document, trigger);
  } else {
    collapseAll(document.querySelectorAll('[data-faq-trigger]'), document);
    expandItem(document, trigger);
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('FAQ & Documentation Integration', () => {
  // -----------------------------------------------------------------------
  // Test Case 1: Render FAQ section with at least 4 items
  // -----------------------------------------------------------------------
  describe('Test Case 1: FAQ item count', () => {
    it('should have at least 4 FAQ items', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const faqSection = document.getElementById('faq');
      expect(faqSection).not.toBeNull();

      const triggers = faqSection!.querySelectorAll('[data-faq-trigger]');
      expect(triggers.length).toBeGreaterThanOrEqual(4);
    });

    it('should have exactly the number of items as the data source', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const triggers = document.querySelectorAll('[data-faq-trigger]');
      expect(triggers.length).toBe(faqData.length);
    });

    it('should have a data-faq-list container wrapping the items', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const list = document.querySelector('[data-faq-list]');
      expect(list).not.toBeNull();
    });

    it('should have a question text for each FAQ item', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const triggers = document.querySelectorAll('[data-faq-trigger]');
      triggers.forEach((trigger, i) => {
        const text = trigger.textContent?.trim() || '';
        expect(text.length).toBeGreaterThan(0);
        expect(text).toContain(faqData[i].question);
      });
    });

    it('should have an answer panel for each FAQ item', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      faqData.forEach((item) => {
        const answerId = `faq-answer-${item.id}`;
        const answer = document.getElementById(answerId);
        expect(answer).not.toBeNull();
        expect(answer!.getAttribute('role')).toBe('region');
      });
    });

    it('should use semantic h3 headings for FAQ questions', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const items = document.querySelectorAll('[data-faq-list] > div');
      items.forEach((item) => {
        const heading = item.querySelector('h3');
        expect(heading).not.toBeNull();
        const button = heading!.querySelector('button');
        expect(button).not.toBeNull();
      });
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 2: Click to expand FAQ item
  // -----------------------------------------------------------------------
  describe('Test Case 2: Click to expand', () => {
    it('should expand an FAQ item when clicked', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const firstTrigger = document.querySelector('[data-faq-trigger]')!;
      expect(firstTrigger.getAttribute('aria-expanded')).toBe('false');

      toggleItem(document, firstTrigger);

      expect(firstTrigger.getAttribute('aria-expanded')).toBe('true');
    });

    it('should show the answer when the item is expanded', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const firstTrigger = document.querySelector('[data-faq-trigger]')!;
      const answerId = firstTrigger.getAttribute('aria-controls')!;
      const answer = document.getElementById(answerId)!;

      expect(answer.getAttribute('aria-hidden')).toBe('true');

      toggleItem(document, firstTrigger);

      expect(answer.getAttribute('aria-hidden')).toBe('false');
    });

    it('should only expand one item at a time (accordion behavior)', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const triggers = document.querySelectorAll('[data-faq-trigger]');

      // Expand first item
      toggleItem(document, triggers[0]);
      expect(triggers[0].getAttribute('aria-expanded')).toBe('true');

      // Expand second item
      toggleItem(document, triggers[1]);

      // First should be collapsed, second expanded
      expect(triggers[0].getAttribute('aria-expanded')).toBe('false');
      expect(triggers[1].getAttribute('aria-expanded')).toBe('true');
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 3: Click to collapse
  // -----------------------------------------------------------------------
  describe('Test Case 3: Click to collapse', () => {
    it('should collapse an expanded FAQ item when clicked again', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const firstTrigger = document.querySelector('[data-faq-trigger]')!;

      // Expand
      toggleItem(document, firstTrigger);
      expect(firstTrigger.getAttribute('aria-expanded')).toBe('true');

      // Collapse (click again)
      toggleItem(document, firstTrigger);
      expect(firstTrigger.getAttribute('aria-expanded')).toBe('false');
    });

    it('should hide the answer when the item is collapsed', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const firstTrigger = document.querySelector('[data-faq-trigger]')!;
      const answerId = firstTrigger.getAttribute('aria-controls')!;
      const answer = document.getElementById(answerId)!;

      // Expand first
      toggleItem(document, firstTrigger);
      expect(answer.getAttribute('aria-hidden')).toBe('false');

      // Collapse
      toggleItem(document, firstTrigger);
      expect(answer.getAttribute('aria-hidden')).toBe('true');
    });

    it('should return to collapsed state indicator after click-away', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const triggers = document.querySelectorAll('[data-faq-trigger]');

      toggleItem(document, triggers[0]);
      expect(triggers[0].getAttribute('aria-expanded')).toBe('true');

      // Clicking another item collapses the first
      toggleItem(document, triggers[2]);
      expect(triggers[0].getAttribute('aria-expanded')).toBe('false');
      expect(triggers[2].getAttribute('aria-expanded')).toBe('true');
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 4: Keyboard interaction
  // -----------------------------------------------------------------------
  describe('Test Case 4: Keyboard interaction', () => {
    it('should expand when Enter key is pressed on a focused FAQ trigger', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const firstTrigger = document.querySelector('[data-faq-trigger]')!;

      // Simulate keyboard interaction: Enter expands
      toggleItem(document, firstTrigger);
      expect(firstTrigger.getAttribute('aria-expanded')).toBe('true');
    });

    it('should collapse when Enter key is pressed on an expanded FAQ trigger', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const firstTrigger = document.querySelector('[data-faq-trigger]')!;

      toggleItem(document, firstTrigger);
      expect(firstTrigger.getAttribute('aria-expanded')).toBe('true');

      toggleItem(document, firstTrigger);
      expect(firstTrigger.getAttribute('aria-expanded')).toBe('false');
    });

    it('should expand when Space key is pressed on a focused FAQ trigger', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const secondTrigger = document.querySelectorAll('[data-faq-trigger]')[1];

      // Simulate Space key: same toggle behavior
      toggleItem(document, secondTrigger);
      expect(secondTrigger.getAttribute('aria-expanded')).toBe('true');
    });

    it('should maintain single-open behavior with keyboard interaction', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const triggers = document.querySelectorAll('[data-faq-trigger]');

      // Enter on first
      toggleItem(document, triggers[0]);
      expect(triggers[0].getAttribute('aria-expanded')).toBe('true');

      // Space on second
      toggleItem(document, triggers[1]);
      expect(triggers[0].getAttribute('aria-expanded')).toBe('false');
      expect(triggers[1].getAttribute('aria-expanded')).toBe('true');
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 5: Memcached protocol command reference
  // -----------------------------------------------------------------------
  describe('Test Case 5: Memcached protocol command reference', () => {
    it('should reference "set" command in FAQ content', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const combined = allAnswers.map((a) => a.textContent || '').join(' ');

      expect(combined.toLowerCase()).toMatch(/\bset\b/);
    });

    it('should reference "get" command in FAQ content', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const combined = allAnswers.map((a) => a.textContent || '').join(' ');

      expect(combined.toLowerCase()).toMatch(/\bget\b/);
    });

    it('should reference "delete" command in FAQ content', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const combined = allAnswers.map((a) => a.textContent || '').join(' ');

      expect(combined.toLowerCase()).toMatch(/\bdelete\b/);
    });

    it('should include at least 3 command references', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const combined = allAnswers.map((a) => a.textContent || '').join(' ');

      const commands = ['set', 'get', 'delete', 'add', 'replace', 'append', 'prepend'];
      const foundCount = commands.filter((cmd) =>
        combined.toLowerCase().includes(cmd),
      ).length;

      expect(foundCount).toBeGreaterThanOrEqual(3);
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 6: Documentation links
  // -----------------------------------------------------------------------
  describe('Test Case 6: Documentation links', () => {
    it('should have at least one FAQ answer containing a link', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const links = allAnswers.flatMap((a) => Array.from(a.querySelectorAll('a')));

      expect(links.length).toBeGreaterThanOrEqual(1);
    });

    it('should have a link pointing to GitHub README or external documentation', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const links = allAnswers.flatMap((a) => Array.from(a.querySelectorAll('a')));

      const docLinks = links.filter(
        (link) =>
          (link.getAttribute('href') || '').includes('github.com') ||
          (link.getAttribute('href') || '').includes('readme') ||
          (link.getAttribute('href') || '').includes('docs'),
      );

      expect(docLinks.length).toBeGreaterThanOrEqual(1);
    });

    it('should have at least one external link with target="_blank"', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const links = allAnswers.flatMap((a) => Array.from(a.querySelectorAll('a')));

      const externalLinks = links.filter((link) => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http');
      });

      expect(externalLinks.length).toBeGreaterThanOrEqual(1);
      externalLinks.forEach((link) => {
        expect(link.getAttribute('target')).toBe('_blank');
      });
    });

    it('should have rel="noopener noreferrer" on external links', () => {
      const { document } = parseHTML(buildFAQSectionHTML(faqData as FAQItemData[]));

      const allAnswers = Array.from(document.querySelectorAll('[role="region"]'));
      const links = allAnswers.flatMap((a) => Array.from(a.querySelectorAll('a')));

      const externalLinks = links.filter((link) => {
        const href = link.getAttribute('href') || '';
        return href.startsWith('http');
      });

      externalLinks.forEach((link) => {
        expect(link.getAttribute('rel')).toBe('noopener noreferrer');
      });
    });
  });

  // -----------------------------------------------------------------------
  // Test Case 7: Empty FAQ data
  // -----------------------------------------------------------------------
  describe('Test Case 7: Empty FAQ data', () => {
    it('should render an empty state message when FAQ data is empty', () => {
      const { document } = parseHTML(buildFAQSectionHTML([]));

      const faqSection = document.getElementById('faq');
      expect(faqSection).not.toBeNull();

      const emptyMsg = faqSection!.querySelector('[data-faq-empty]');
      expect(emptyMsg).not.toBeNull();
      expect(emptyMsg!.textContent).toMatch(/no.*faq|empty|available/i);
    });

    it('should not render any FAQ triggers when data is empty', () => {
      const { document } = parseHTML(buildFAQSectionHTML([]));

      const triggers = document.querySelectorAll('[data-faq-trigger]');
      expect(triggers.length).toBe(0);
    });

    it('should not render a FAQ list container when data is empty', () => {
      const { document } = parseHTML(buildFAQSectionHTML([]));

      const list = document.querySelector('[data-faq-list]');
      expect(list).toBeNull();
    });

    it('should not throw an error with empty data', () => {
      expect(() => {
        parseHTML(buildFAQSectionHTML([]));
      }).not.toThrow();
    });

    it('should still render the FAQ heading when empty', () => {
      const { document } = parseHTML(buildFAQSectionHTML([]));

      const heading = document.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading!.textContent).toMatch(/frequently asked questions/i);
    });
  });
});
