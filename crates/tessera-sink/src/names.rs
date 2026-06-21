//! Per-region description derivation.
//!
//! A Sink namespaces its three region families under one base
//! description. The owner derives these strings and hands the exact
//! derived strings to each worker over argv, so both sides feed the
//! identical string into BLAKE3 and agree on the SHM region without
//! the worker needing to re-run this logic.

/// Pool region description: `"<base>/pool"`.
pub fn pool(base: &str) -> String {
    format!("{base}/pool")
}

/// Shared ack-channel description: `"<base>/ack"`.
pub fn ack(base: &str) -> String {
    format!("{base}/ack")
}

/// Per-worker control-channel description: `"<base>/control/<worker_id>"`.
pub fn control(base: &str, worker_id: u32) -> String {
    format!("{base}/control/{worker_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivations_are_distinct_and_stable() {
        let base = "app/artifacts";
        assert_eq!(pool(base), "app/artifacts/pool");
        assert_eq!(ack(base), "app/artifacts/ack");
        assert_eq!(control(base, 0), "app/artifacts/control/0");
        assert_eq!(control(base, 3), "app/artifacts/control/3");
        // All four are distinct so their BLAKE3 handles never collide.
        let all = [pool(base), ack(base), control(base, 0), control(base, 1)];
        for (i, a) in all.iter().enumerate() {
            for b in &all[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
