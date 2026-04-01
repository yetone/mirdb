/**
 * Vitest type augmentation for @testing-library/jest-dom matchers.
 * Owner: Scenario 18 - TypeScript Type Safety
 *
 * This file extends Vitest's Assertion interface with jest-dom matchers
 * so TypeScript recognizes toBeInTheDocument, toHaveAttribute, etc.
 */

/// <reference types="@testing-library/jest-dom" />

import '@testing-library/jest-dom'

declare module '@testing-library/jest-dom/matchers' {
  export interface TestingLibraryMatchers<R = void, T = {}> {
    toBeInTheDocument(): R
    toBeVisible(): R
    toBeEmpty(): R
    toBeDisabled(): R
    toBeEnabled(): R
    toBeInvalid(): R
    toBeRequired(): R
    toBeValid(): R
    toContainElement(element: HTMLElement | SVGElement | null): R
    toContainHTML(html: string): R
    toHaveAttribute(attr: string, value?: string | RegExp): R
    toHaveClass(...classNames: string[]): R
    toHaveFocus(): R
    toHaveFormValues(expectedValues: Record<string, unknown>): R
    toHaveStyle(css: string | Record<string, unknown>): R
    toHaveTextContent(
      text: string | RegExp,
      options?: { normalizeWhitespace: boolean }
    ): R
    toHaveValue(value?: string | string[] | number | null): R
    toHaveDisplayValue(value: string | RegExp | Array<string | RegExp>): R
    toBeChecked(): R
    toBePartiallyChecked(): R
    toHaveErrorMessage(text?: string | RegExp): R
    toHaveAccessibleDescription(text?: string | RegExp): R
    toHaveAccessibleName(text?: string | RegExp): R
  }
}

declare module 'vitest' {
  interface Assertion<T = unknown> extends jest.Matchers<void, T> {
    toBeInTheDocument(): T
    toBeVisible(): T
    toBeEmpty(): T
    toBeDisabled(): T
    toBeEnabled(): T
    toBeInvalid(): T
    toBeRequired(): T
    toBeValid(): T
    toContainElement(element: HTMLElement | SVGElement | null): T
    toContainHTML(html: string): T
    toHaveAttribute(attr: string, value?: string | RegExp): T
    toHaveClass(...classNames: string[]): T
    toHaveFocus(): T
    toHaveFormValues(expectedValues: Record<string, unknown>): T
    toHaveStyle(css: string | Record<string, unknown>): T
    toHaveTextContent(
      text: string | RegExp,
      options?: { normalizeWhitespace: boolean }
    ): T
    toHaveValue(value?: string | string[] | number | null): T
    toHaveDisplayValue(value: string | RegExp | Array<string | RegExp>): T
    toBeChecked(): T
    toBePartiallyChecked(): T
    toHaveErrorMessage(text?: string | RegExp): T
    toHaveAccessibleDescription(text?: string | RegExp): T
    toHaveAccessibleName(text?: string | RegExp): T
  }
  interface AsymmetricMatchersContaining {
    toBeInTheDocument(): unknown
    toBeVisible(): unknown
    toBeEmpty(): unknown
    toBeDisabled(): unknown
    toBeEnabled(): unknown
    toHaveAttribute(attr: string, value?: string | RegExp): unknown
    toHaveClass(...classNames: string[]): unknown
    toHaveTextContent(
      text: string | RegExp,
      options?: { normalizeWhitespace: boolean }
    ): unknown
    toHaveStyle(css: string | Record<string, unknown>): unknown
  }
}

export {}
