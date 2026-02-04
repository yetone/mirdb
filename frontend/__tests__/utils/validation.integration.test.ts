/**
 * Integration tests for URL validation behavior.
 * Owner: Scenario 3 - URL Input Validation
 *
 * Tests cover:
 * - Test Case 6: Submit form with invalid URL - error displayed, no API call
 * - Test Case 7: Error clears when valid URL entered
 *
 * These tests simulate form validation behavior using the validation utilities.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { validateUrl } from '../../src/utils/validation';

describe('Form Validation Integration', () => {
  describe('Test Case 6: Submit form with invalid URL', () => {
    it('should show error and prevent API call for ftp:// URL', () => {
      const mockApiCall = vi.fn();
      const url = 'ftp://invalid';

      // Simulate form validation before API call
      const validationResult = validateUrl(url);

      if (validationResult.isValid) {
        mockApiCall(url);
      }

      expect(validationResult.isValid).toBe(false);
      expect(validationResult.errorMessage).toBe('Please enter a valid URL');
      expect(mockApiCall).not.toHaveBeenCalled();
    });

    it('should prevent API call for URL without protocol', () => {
      const mockApiCall = vi.fn();
      const url = 'invalid.com';

      const validationResult = validateUrl(url);

      if (validationResult.isValid) {
        mockApiCall(url);
      }

      expect(validationResult.isValid).toBe(false);
      expect(mockApiCall).not.toHaveBeenCalled();
    });

    it('should prevent API call for empty URL', () => {
      const mockApiCall = vi.fn();
      const url = '';

      const validationResult = validateUrl(url);

      if (validationResult.isValid) {
        mockApiCall(url);
      }

      expect(validationResult.isValid).toBe(false);
      expect(mockApiCall).not.toHaveBeenCalled();
    });

    it('should allow API call only for valid URLs', () => {
      const mockApiCall = vi.fn();
      const url = 'https://example.com';

      const validationResult = validateUrl(url);

      if (validationResult.isValid) {
        mockApiCall(url);
      }

      expect(validationResult.isValid).toBe(true);
      expect(mockApiCall).toHaveBeenCalledWith(url);
    });
  });

  describe('Test Case 7: Error clears when valid URL entered', () => {
    it('should transition from error to valid state when URL is corrected', () => {
      // Simulate form state tracking
      let errorMessage: string | null = null;

      // Step 1: User enters invalid URL
      const invalidResult = validateUrl('example.com');
      errorMessage = invalidResult.errorMessage ?? null;

      expect(errorMessage).toBe('URL must include protocol (http:// or https://)');

      // Step 2: User corrects URL by adding protocol
      const validResult = validateUrl('https://example.com');
      errorMessage = validResult.errorMessage ?? null;

      expect(errorMessage).toBeNull();
      expect(validResult.isValid).toBe(true);
    });

    it('should clear error when empty input becomes valid URL', () => {
      let errorMessage: string | null = null;

      // Step 1: User submits empty
      const emptyResult = validateUrl('');
      errorMessage = emptyResult.errorMessage ?? null;

      expect(errorMessage).toBe('Please enter a URL');

      // Step 2: User types valid URL
      const validResult = validateUrl('http://localhost:3000');
      errorMessage = validResult.errorMessage ?? null;

      expect(errorMessage).toBeNull();
    });

    it('should show different error messages as user types', () => {
      // Simulate progressive typing
      const inputs = [
        { input: '', expectedError: 'Please enter a URL' },
        { input: 'e', expectedError: 'Please enter a valid URL' },
        { input: 'example', expectedError: 'Please enter a valid URL' },
        { input: 'example.com', expectedError: 'URL must include protocol (http:// or https://)' },
        { input: 'https://example.com', expectedError: null },
      ];

      inputs.forEach(({ input, expectedError }) => {
        const result = validateUrl(input);
        expect(result.errorMessage ?? null).toBe(expectedError);
      });
    });

    it('should handle real-time validation state changes', () => {
      // Simulate a form controller tracking validation state
      interface FormState {
        value: string;
        error: string | null;
        isValid: boolean;
      }

      function updateFormState(value: string): FormState {
        const result = validateUrl(value);
        return {
          value,
          error: result.errorMessage ?? null,
          isValid: result.isValid,
        };
      }

      // Initial empty state
      let state = updateFormState('');
      expect(state.error).toBe('Please enter a URL');
      expect(state.isValid).toBe(false);

      // User types invalid URL
      state = updateFormState('invalid');
      expect(state.error).toBe('Please enter a valid URL');
      expect(state.isValid).toBe(false);

      // User corrects to valid URL
      state = updateFormState('https://example.com');
      expect(state.error).toBeNull();
      expect(state.isValid).toBe(true);

      // User clears input again
      state = updateFormState('');
      expect(state.error).toBe('Please enter a URL');
      expect(state.isValid).toBe(false);
    });
  });
});
