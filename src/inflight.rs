//! Messages sent but not yet acked, and what QuestDB's responses mean for them.
//!
//! The server numbers inbound frames from 0 per connection. An OK carries a **cumulative**
//! watermark: `OK seq=N` acks every frame up to N, and the server coalesces them (at least every
//! 64 frames, and whenever a socket read ends), so one OK often covers several messages. An error
//! names exactly one frame; the server first flushes the OK for everything before it, then refuses
//! every later frame on the connection without answering. So after an error the connection is
//! done: everything from the failed frame on is replayed on a fresh one.

/// Unacked messages, oldest first, as the number of ring records each carried (0 for a priority
/// record, which isn't kept for re-sending).
pub(crate) struct Inflight<const N: usize> {
    records: [usize; N],
    len: usize,
    next_seq: i64,
}

impl<const N: usize> Inflight<N> {
    pub(crate) fn new() -> Self {
        Self {
            records: [0; N],
            len: 0,
            next_seq: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_full(&self) -> bool {
        self.len == N
    }

    /// Ring records in flight: the ones after them are unsent.
    pub(crate) fn records(&self) -> usize {
        self.records[..self.len].iter().sum()
    }

    pub(crate) fn push(&mut self, records: usize) {
        self.records[self.len] = records;
        self.len += 1;
    }

    /// The sequence the oldest unacked message got.
    pub(crate) fn next_seq(&self) -> i64 {
        self.next_seq
    }

    /// `OK seq`: retires every message up to `seq`; returns how many ring records they carried.
    /// None if `seq` isn't a message in flight.
    pub(crate) fn ok(&mut self, seq: i64) -> Option<usize> {
        let k = usize::try_from(seq - self.next_seq).ok()? + 1;
        (k <= self.len).then(|| self.retire(k))
    }

    /// An error on `seq`: it must be the oldest in flight, since the server acks everything before
    /// it first. Returns its ring records. The caller ends the connection either way.
    pub(crate) fn error(&mut self, seq: i64) -> Option<usize> {
        (seq == self.next_seq && self.len > 0).then(|| self.retire(1))
    }

    fn retire(&mut self, k: usize) -> usize {
        let n = self.records[..k].iter().sum();
        self.records.copy_within(k..self.len, 0);
        self.len -= k;
        self.next_seq += k as i64;
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_ok_covers_several() {
        let mut f = Inflight::<8>::new();
        f.push(0); // a priority record
        f.push(5);
        f.push(7);
        assert_eq!(f.records(), 12);
        assert_eq!(f.ok(1), Some(5)); // seq 0 and 1 in one ack
        assert_eq!((f.len(), f.next_seq(), f.records()), (1, 2, 7));
        assert_eq!(f.ok(2), Some(7));
        assert_eq!(f.len(), 0);
    }

    #[test]
    fn ok_outside_the_window_is_an_error() {
        let mut f = Inflight::<8>::new();
        f.push(3);
        assert_eq!(f.ok(1), None); // never sent
        f.ok(0).unwrap();
        assert_eq!(f.ok(0), None); // already acked
    }

    #[test]
    fn error_names_the_oldest() {
        let mut f = Inflight::<8>::new();
        f.push(3);
        f.push(4);
        assert_eq!(f.error(1), None); // the server would have acked 0 first
        assert_eq!(f.ok(0), Some(3));
        assert_eq!(f.error(1), Some(4));
        assert!(!f.is_full());
    }
}
