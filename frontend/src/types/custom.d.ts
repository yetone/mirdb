/**
 * Custom Type Definitions
 *
 * Global type definitions for the application.
 */

declare module '*.svg' {
  import * as React from 'react'
  export const ReactComponent: React.FunctionComponent<React.SVGProps<SVGSVGElement>>
  const src: string
  export default src
}

declare module '*.png' {
  const content: string
  export default content
}

declare module '*.jpg' {
  const content: string
  export default content
}

// Extend window for development
interface Window {
  __REDUX_DEVTOOLS_EXTENSION__?: () => unknown
}
