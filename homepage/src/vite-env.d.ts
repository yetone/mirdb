/// <reference types="vite/client" />

declare module '*.css' {
  const content: string
  export default content
}

declare module 'prismjs/themes/*' {
  const content: string
  export default content
}

declare module 'prismjs/components/*' {
  const content: any
  export default content
}
