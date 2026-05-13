export function useClipboard(): { copy: (text: string) => Promise<boolean>; copied: boolean } {
  // To be implemented by Scenario 4
  return { copy: async () => false, copied: false };
}
