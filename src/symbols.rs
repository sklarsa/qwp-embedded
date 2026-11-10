//! The connection's symbol dictionary, as a table the application can add to while the sender
//! runs: a gateway learns device names as devices show up.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering::*};

use embassy_sync::blocking_mutex::{Mutex, raw::CriticalSectionRawMutex};

pub const SYMBOLS_MAX: usize = 32;
pub const SYMBOL_LEN_MAX: usize = 24;

/// Append-only: an entry never changes once added, so readers take no lock and hold `&str`s into
/// it freely. Only `intern` locks, against another `intern`.
pub struct Symbols {
    names: UnsafeCell<[[u8; SYMBOL_LEN_MAX]; SYMBOLS_MAX]>,
    lens: [AtomicU8; SYMBOLS_MAX],
    len: AtomicUsize,
    writer: Mutex<CriticalSectionRawMutex, ()>,
}

// SAFETY: entries below `len` are immutable; writes happen above it, under `writer`, and are
// published by the Release store of `len`.
unsafe impl Sync for Symbols {}

impl Symbols {
    pub const fn new() -> Self {
        Self {
            names: UnsafeCell::new([[0; SYMBOL_LEN_MAX]; SYMBOLS_MAX]),
            lens: [const { AtomicU8::new(0) }; SYMBOLS_MAX],
            len: AtomicUsize::new(0),
            writer: Mutex::new(()),
        }
    }

    /// `s`'s id: the existing one, or the next free. `None` when the table is full or `s` is
    /// longer than `SYMBOL_LEN_MAX`.
    pub fn intern(&self, s: &str) -> Option<u64> {
        if s.len() > SYMBOL_LEN_MAX {
            return None;
        }
        self.writer.lock(|_| {
            let n = self.len.load(Acquire);
            if let Some(i) = (0..n).find(|&i| self.get(i) == s) {
                return Some(i as u64);
            }
            if n == SYMBOLS_MAX {
                return None;
            }
            // SAFETY: slot n is above len, so no reader looks at it; we hold the writer lock
            unsafe { (&mut *self.names.get())[n][..s.len()].copy_from_slice(s.as_bytes()) };
            self.lens[n].store(s.len() as u8, Relaxed);
            self.len.store(n + 1, Release);
            Some(n as u64)
        })
    }

    pub fn len(&self) -> usize {
        self.len.load(Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every name, in id order.
    pub fn with<R>(&self, f: impl FnOnce(&[&str]) -> R) -> R {
        let n = self.len();
        let mut all = [""; SYMBOLS_MAX];
        for (i, slot) in all[..n].iter_mut().enumerate() {
            *slot = self.get(i);
        }
        f(&all[..n])
    }

    fn get(&self, i: usize) -> &str {
        let l = self.lens[i].load(Relaxed) as usize;
        // SAFETY: i < len, so the entry is immutable and was written from a &str
        unsafe { core::str::from_utf8_unchecked(&(&*self.names.get())[i][..l]) }
    }
}

impl Default for Symbols {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_is_stable() {
        let s = Symbols::new();
        assert_eq!(s.intern("a1b2c3"), Some(0));
        assert_eq!(s.intern("0.3.0"), Some(1));
        assert_eq!(s.intern("a1b2c3"), Some(0));
        assert_eq!(s.len(), 2);
        s.with(|n| assert_eq!(n, ["a1b2c3", "0.3.0"]));
    }

    #[test]
    fn intern_full_returns_none() {
        let s = Symbols::new();
        for i in 0..32 {
            assert!(s.intern(&format!("n{i}")).is_some());
        }
        assert_eq!(s.intern("one-more"), None);
        assert_eq!(s.intern("n0"), Some(0));
        assert_eq!(s.intern(&"x".repeat(25)), None);
    }
}
