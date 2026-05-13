import '@testing-library/jest-dom';

function evalMediaQuery(query: string, width: number): boolean {
  const minMatch = query.match(/min-width:\s*(\d+)px/);
  const maxMatch = query.match(/max-width:\s*(\d+)px/);
  if (minMatch && maxMatch) {
    const min = parseInt(minMatch[1], 10);
    const max = parseInt(maxMatch[1], 10);
    return width >= min && width <= max;
  }
  if (minMatch) {
    return width >= parseInt(minMatch[1], 10);
  }
  if (maxMatch) {
    return width <= parseInt(maxMatch[1], 10);
  }
  return false;
}

interface MockMQL {
  matches: boolean;
  media: string;
  onchange: ((e: MediaQueryListEvent) => void) | null;
  addListener: (fn: (e: MediaQueryListEvent) => void) => void;
  removeListener: () => void;
  addEventListener: (type: string, fn: EventListenerOrEventListenerObject) => void;
  removeEventListener: () => void;
  dispatchEvent: (e: Event) => boolean;
  _listeners: Array<(e: MediaQueryListEvent) => void>;
}

const allMQLs: MockMQL[] = [];

function notifyMQLChanges() {
  const width = window.innerWidth;
  for (const mql of allMQLs) {
    const newMatches = evalMediaQuery(mql.media, width);
    if (newMatches !== mql.matches) {
      mql.matches = newMatches;
      const event = new Event('change') as MediaQueryListEvent;
      Object.defineProperty(event, 'matches', { value: newMatches });
      Object.defineProperty(event, 'media', { value: mql.media });
      mql.dispatchEvent(event);
    }
  }
}

// Export for use in tests that need to manually trigger MQL re-evaluation
(globalThis as any).__notifyMQLChanges = notifyMQLChanges;

// Mock window.matchMedia for JSDOM with query evaluation
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: (query: string) => {
    const listeners: Array<(e: MediaQueryListEvent) => void> = [];
    const mql: MockMQL = {
      matches: evalMediaQuery(query, window.innerWidth),
      media: query,
      onchange: null,
      _listeners: listeners,
      addListener: (fn: (e: MediaQueryListEvent) => void) => listeners.push(fn),
      removeListener: () => {},
      addEventListener: (_type: string, fn: EventListenerOrEventListenerObject) => {
        listeners.push(fn as (e: MediaQueryListEvent) => void);
      },
      removeEventListener: () => {},
      dispatchEvent: (e: Event) => {
        listeners.forEach((fn) => fn(e as MediaQueryListEvent));
        if (mql.onchange) mql.onchange(e as MediaQueryListEvent);
        return true;
      },
    };
    allMQLs.push(mql);
    return mql;
  },
});

// Intercept window.dispatchEvent to re-evaluate MQLs on resize
const origDispatchEvent = window.dispatchEvent.bind(window);
window.dispatchEvent = function (event: Event) {
  const result = origDispatchEvent(event);
  if (event.type === 'resize') {
    notifyMQLChanges();
  }
  return result;
};
