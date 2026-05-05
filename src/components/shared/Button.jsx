/**
 * Consistent button styling and behavior
 * Owner: Scenario 5 - Navigation and External Links
 *
 * Expected exports:
 * - Button: React component
 *
 * Requirements:
 * - Consistent styling across all navigation elements
 * - Supports primary and secondary visual variants
 * - Accessible with proper ARIA attributes
 */

import React from 'react';

export const Button = ({ children, variant = 'primary', ...props }) => {
  const baseClasses = "px-4 py-2 rounded-lg font-medium transition-colors duration-200";
  const variantClasses = {
    primary: "bg-blue-600 text-white hover:bg-blue-700 focus:ring-2 focus:ring-blue-500",
    secondary: "bg-gray-200 text-gray-800 hover:bg-gray-300 focus:ring-2 focus:ring-gray-400"
  };

  return (
    <button
      className={`${baseClasses} ${variantClasses[variant]}`}
      aria-label={props['aria-label'] || children}
      {...props}
    >
      {children}
    </button>
  );
};