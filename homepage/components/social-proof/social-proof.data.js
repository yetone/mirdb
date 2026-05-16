/*
 * Default social-proof data — Scenario 9.
 *
 * Exports:
 *   SOCIAL_PROOF_DATA: {
 *     testimonials: Array<{ author: string, body: string, role?: string }>,
 *     badges: Array<{ src: string, alt: string, name?: string }>,
 *     userCount: number | null
 *   }
 *
 * Per PRD REQ-8 this section is a Should-have; consumers may pass empty data
 * to opt out, in which case the renderer hides the section gracefully.
 */

export const SOCIAL_PROOF_DATA = {
    testimonials: [
        {
            author: "Avery K., Backend Engineer",
            body: "Dropping MirDB in front of our existing memcached clients was a five-minute change. We got durability for free.",
        },
        {
            author: "Priya S., SRE",
            body: "Sub-millisecond GET latency at p99 under our production load. The LSM compaction story is rock solid.",
        },
        {
            author: "Jordan T., Tech Lead",
            body: "Written in Rust, predictable memory, no GC pauses. Exactly what we wanted from a key-value tier.",
        },
    ],
    badges: [
        {
            src: "images/social-proof/rust-foundation.svg",
            alt: "Rust Foundation member badge",
            name: "Rust Foundation",
        },
        {
            src: "images/social-proof/oss-100.svg",
            alt: "Open Source 100% certified badge",
            name: "Open Source 100",
        },
        {
            src: "images/social-proof/security-audit.svg",
            alt: "Independent security audit completed badge",
            name: "Security Audited",
        },
    ],
    userCount: 12500,
};

export const EMPTY_SOCIAL_PROOF_DATA = {
    testimonials: [],
    badges: [],
    userCount: null,
};
