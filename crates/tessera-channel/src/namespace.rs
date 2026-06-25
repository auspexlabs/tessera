//! Namespace identification — BLAKE3-derived region handle.
//!
//! Same pattern as Pool / Ring: the public surface uses a
//! human-readable `description` string; internally we hash it with
//! BLAKE3 to derive a stable, deterministic handle. Two peers with
//! the same description derive the same handle and attach to the
//! same SHM region with no manual coordination.
//!
//! POSIX SHM segment name: `/tessera-channel-<32 hex chars>` (the
//! first 128 bits of the BLAKE3 digest), which fits in NAME_MAX
//! with room to spare and stays disjoint from Pool's
//! `/tessera-pool-…` and Ring's `/tessera-ring-…` prefixes.

use blake3::Hasher;

/// 128-bit prefix of BLAKE3(description), encoded as 32 hex chars
/// and used as the POSIX SHM region name suffix.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NamespaceHandle {
    /// First 16 bytes of BLAKE3(description).
    digest_prefix: [u8; 16],
    /// Full 32-byte BLAKE3 digest. Stored in the SHM header so
    /// attachers can cross-verify their description against the
    /// creator's.
    full_digest: [u8; 32],
}

impl NamespaceHandle {
    /// Derive a namespace handle from the operator-facing description.
    pub fn derive(description: &str) -> Self {
        let mut h = Hasher::new();
        h.update(description.as_bytes());
        let full = h.finalize();
        let full_digest: [u8; 32] = *full.as_bytes();
        let mut digest_prefix = [0u8; 16];
        digest_prefix.copy_from_slice(&full_digest[..16]);
        Self {
            digest_prefix,
            full_digest,
        }
    }

    /// Full BLAKE3 digest for header storage / cross-verification.
    pub fn full_digest(&self) -> [u8; 32] {
        self.full_digest
    }

    /// POSIX SHM region name (`/tessera-channel-<hex>`).
    pub fn shm_name(&self) -> String {
        let mut out = String::from("/tessera-channel-");
        for byte in &self.digest_prefix {
            use core::fmt::Write;
            // Safe: writing to a String. Unwrap is unreachable.
            write!(&mut out, "{byte:02x}").unwrap();
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_description_derives_same_handle() {
        let a = NamespaceHandle::derive("my-app/control");
        let b = NamespaceHandle::derive("my-app/control");
        assert_eq!(a.full_digest(), b.full_digest());
        assert_eq!(a.shm_name(), b.shm_name());
    }

    #[test]
    fn different_descriptions_derive_different_handles() {
        let a = NamespaceHandle::derive("my-app/control");
        let b = NamespaceHandle::derive("my-app/ack");
        assert_ne!(a.full_digest(), b.full_digest());
        assert_ne!(a.shm_name(), b.shm_name());
    }

    #[test]
    fn shm_name_has_expected_shape() {
        let h = NamespaceHandle::derive("test");
        let name = h.shm_name();
        // "/tessera-channel-" + 32 hex chars
        assert!(name.starts_with("/tessera-channel-"));
        assert_eq!(name.len(), "/tessera-channel-".len() + 32);
        let hex = &name["/tessera-channel-".len()..];
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn channel_pool_ring_handles_do_not_collide_for_same_description() {
        // Same description but different SHM-name prefix: channel,
        // ring, and pool regions can coexist for the same
        // description without name collision.
        let h = NamespaceHandle::derive("shared-description");
        assert!(h.shm_name().starts_with("/tessera-channel-"));
        assert!(!h.shm_name().starts_with("/tessera-pool-"));
        assert!(!h.shm_name().starts_with("/tessera-ring-"));
    }

    #[test]
    fn empty_description_is_handled() {
        let h = NamespaceHandle::derive("");
        let _ = h.shm_name();
    }
}
