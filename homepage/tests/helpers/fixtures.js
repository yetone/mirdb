/**
 * Reusable fixtures for tests.
 * Created by the first scenario builder.
 */

const sampleContent = {
  hero: {
    title: 'MirDB',
    tagline: 'Persistent key-value store',
    ctas: [
      { label: 'View on GitHub', href: 'https://example.test/repo', primary: true },
    ],
  },
  codeExample: {
    language: 'shell',
    description: 'Sample',
    snippet: 'set hello 0 0 5\nworld\nget hello\n',
  },
};

function buildClipboardMock({ shouldReject = false, rejectError } = {}) {
  return {
    writeText: jest.fn(() =>
      shouldReject
        ? Promise.reject(rejectError || new Error('clipboard denied'))
        : Promise.resolve(undefined)
    ),
  };
}

// REQ-3 feature identifiers — every entry must surface as both a card in
// the features grid and an item in src/data/content.json#features.
const REQUIRED_FEATURE_IDS = [
  'memcached',
  'rust',
  'persistence',
  'lsm-tree',
  'skip-list',
  'wal',
  'compaction',
];

module.exports = { sampleContent, buildClipboardMock, REQUIRED_FEATURE_IDS };
