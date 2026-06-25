//! Tessera Channel — non-lossy MPSC shared-memory queue.
//!
//! See the workspace README and `docs/concept_landscape.md` for the
//! design summary.
//!
//! Channel fills cell #3 of the lossiness × reader-topology ×
//! payload-shape matrix (non-lossy multi-producer single-consumer
//! small-typed-bytes), complementing Pool (cell #5, non-lossy
//! lease-based bulk) and Ring (cell #2, lossy multi-reader broadcast).
//! See `docs/concept_landscape.md` in the workspace root for the full
//! matrix view.
//!
//! The public surface is intentionally small: open a [`Channel`] with a
//! [`ChannelConfig`], then use Sender-role handles to `send` and the
//! single Receiver-role handle to `recv`.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod channel;
pub mod error;
pub mod header;
pub mod namespace;
pub mod region;

pub use channel::Channel;
pub use error::{ChannelRoleSnapshot, Result, TesseraChannelError};
pub use namespace::NamespaceHandle;

/// Role this Channel handle is opened with.
///
/// MPSC: exactly one process holds the `Receiver` role for any
/// given region (that's the region creator + the only consumer
/// allowed to call `recv()`); zero or more processes hold the
/// `Sender` role (they attach to an existing region and call
/// `send()` / `try_send()` / `send_timeout()`).
///
/// Calling a role-mismatched operation (e.g., `send` on a Receiver,
/// or `recv` on a Sender) returns `TesseraChannelError::RoleMismatch`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ChannelRole {
    /// Region creator. Holds exclusive lifecycle (drop unlinks the
    /// SHM name). Only role that may call `recv` / `try_recv`.
    Receiver,
    /// Region attacher. Calls `send` / `try_send` / `send_timeout`.
    /// Multiple Sender processes may coexist (MPSC).
    Sender,
}

impl ChannelRole {
    /// Convert to the small `ChannelRoleSnapshot` enum used in error
    /// variants (avoids importing the full public type into errors).
    pub fn snapshot(self) -> ChannelRoleSnapshot {
        match self {
            ChannelRole::Receiver => ChannelRoleSnapshot::Receiver,
            ChannelRole::Sender => ChannelRoleSnapshot::Sender,
        }
    }
}

/// Configuration for opening a Channel.
///
/// Mirrors Pool / Ring's `*Config` pattern: a description string for
/// BLAKE3 namespace derivation, geometry, role flag, and a
/// force_recreate escape hatch for owner-side recovery from a
/// crashed prior receiver.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    /// Human-readable description; hashed via BLAKE3 into the SHM region name.
    pub description: String,
    /// Number of slots in the ring. Both Receiver and Sender must
    /// supply matching geometry; mismatched config is rejected at
    /// attach time via `HeaderMismatch`.
    pub slot_count: u32,
    /// Per-slot payload capacity in bytes (excludes SlotHeader).
    /// Must be a multiple of 8 for AtomicU64 alignment on the
    /// per-slot `sequence` and `ready` fields (validated at
    /// construction).
    pub slot_size_bytes: u32,
    /// `Receiver` → create the region; `Sender` → attach to an
    /// existing one. MPSC: exactly one Receiver per region; multiple
    /// Senders may coexist.
    pub role: ChannelRole,
    /// Receiver-only recovery escape hatch: if `true` and a region
    /// with the same name already exists, unlink + recreate
    /// unconditionally. Misuse will silently clobber a live Receiver;
    /// only set this during explicit recovery after confirming no
    /// live Receiver exists. Ignored when `role == Sender`.
    pub force_recreate: bool,
}
