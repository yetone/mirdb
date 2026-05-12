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

module.exports = { sampleContent, buildClipboardMock };
