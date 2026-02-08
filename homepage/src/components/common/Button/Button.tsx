/**
 * Reusable Button Component
 * Owner: Scenario 5 - Navigation and Links
 *
 * Expected exports:
 * - Button: React.FC<ButtonProps> - Reusable button component
 * - ButtonProps: interface
 *
 * Features:
 * - Primary and secondary variants
 * - Link button (as anchor tag)
 * - Hover and focus states
 * - Accessible (proper ARIA attributes)
 */

export function Button({ children }: { children: React.ReactNode }) {
  return <button>{children}</button>;
}
