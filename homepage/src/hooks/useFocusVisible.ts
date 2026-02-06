/**
 * Focus visible hook for accessible focus states.
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Provides focus-visible polyfill behavior for better keyboard navigation UX.
 * Only shows focus indicators when user is navigating with keyboard.
 *
 * @returns object with isFocusVisible state and focusProps handlers
 */
import { useState, useCallback, FocusEvent } from 'react';

interface UseFocusVisibleReturn {
  isFocusVisible: boolean;
  focusProps: {
    onFocus: (e: FocusEvent) => void;
    onBlur: () => void;
  };
}

// Track if last interaction was keyboard-based
let hadKeyboardEvent = true;

if (typeof window !== 'undefined') {
  // Listen for tab keydown
  document.addEventListener(
    'keydown',
    (e) => {
      if (e.key === 'Tab') {
        hadKeyboardEvent = true;
      }
    },
    true
  );

  // Listen for pointer events
  document.addEventListener(
    'pointerdown',
    () => {
      hadKeyboardEvent = false;
    },
    true
  );
}

export function useFocusVisible(): UseFocusVisibleReturn {
  const [isFocusVisible, setIsFocusVisible] = useState(false);

  const onFocus = useCallback((e: FocusEvent) => {
    // Only show focus if coming from keyboard navigation
    if (hadKeyboardEvent) {
      setIsFocusVisible(true);
    }

    // Check if focus came from keyboard via focus event target
    if (e.target instanceof HTMLElement) {
      // Some browsers support :focus-visible natively
      try {
        if (e.target.matches(':focus-visible')) {
          setIsFocusVisible(true);
        }
      } catch {
        // :focus-visible not supported, rely on hadKeyboardEvent
        if (hadKeyboardEvent) {
          setIsFocusVisible(true);
        }
      }
    }
  }, []);

  const onBlur = useCallback(() => {
    setIsFocusVisible(false);
  }, []);

  return {
    isFocusVisible,
    focusProps: {
      onFocus,
      onBlur,
    },
  };
}
