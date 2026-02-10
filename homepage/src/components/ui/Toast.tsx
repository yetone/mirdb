/**
 * Toast Notification Component
 * Owner: Scenario 4 - Getting Started Section
 *
 * Displays temporary notifications for:
 * - Copy success feedback
 * - Error messages
 */

import React, { useEffect, useState } from 'react';

export type ToastType = 'success' | 'error' | 'info';

export interface ToastProps {
  message: string;
  type?: ToastType;
  duration?: number;
  onClose?: () => void;
  visible: boolean;
}

const toastStyles: React.CSSProperties = {
  position: 'fixed',
  bottom: '24px',
  right: '24px',
  padding: '12px 20px',
  borderRadius: 'var(--radius-md)',
  boxShadow: 'var(--shadow-lg)',
  display: 'flex',
  alignItems: 'center',
  gap: '8px',
  zIndex: 1000,
  transition: 'all 0.3s ease',
  fontSize: 'var(--font-size-sm)',
  fontWeight: 500,
};

const typeStyles: Record<ToastType, React.CSSProperties> = {
  success: {
    backgroundColor: '#10b981',
    color: '#ffffff',
  },
  error: {
    backgroundColor: '#ef4444',
    color: '#ffffff',
  },
  info: {
    backgroundColor: 'var(--accent-primary)',
    color: '#ffffff',
  },
};

export function Toast({
  message,
  type = 'success',
  duration = 3000,
  onClose,
  visible,
}: ToastProps) {
  const [isVisible, setIsVisible] = useState(visible);

  useEffect(() => {
    setIsVisible(visible);
    if (visible && duration > 0) {
      const timer = setTimeout(() => {
        setIsVisible(false);
        onClose?.();
      }, duration);
      return () => clearTimeout(timer);
    }
  }, [visible, duration, onClose]);

  if (!isVisible) return null;

  return (
    <div
      role="alert"
      aria-live="polite"
      data-testid="toast"
      style={{
        ...toastStyles,
        ...typeStyles[type],
        opacity: isVisible ? 1 : 0,
        transform: isVisible ? 'translateY(0)' : 'translateY(20px)',
      }}
    >
      {type === 'success' && (
        <svg
          width="16"
          height="16"
          viewBox="0 0 16 16"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
          aria-hidden="true"
        >
          <path
            d="M13.5 4.5L6.5 11.5L3 8"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      )}
      {type === 'error' && (
        <svg
          width="16"
          height="16"
          viewBox="0 0 16 16"
          fill="none"
          xmlns="http://www.w3.org/2000/svg"
          aria-hidden="true"
        >
          <path
            d="M12 4L4 12M4 4L12 12"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      )}
      <span>{message}</span>
    </div>
  );
}
