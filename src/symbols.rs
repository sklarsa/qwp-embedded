//! The connection's symbol dictionary, as a table the application can add to while the sender
//! runs: a gateway learns device names as devices show up.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering::*};

use embassy_sync::blocking_mutex::{Mutex, raw::CriticalSectionRawMutex};

/// Up to `N` names of up to `L` bytes (`L` ≤ 255), all in the struct: size both to the
/// application. Append-only: an entry never changes once added, so readers take no lock and hold
/// `&str`s into it freely. Only `intern` locks, against another `intern`.
pub struct Symbols<const N: usize = 32, const L: usize = 24> {
    /// One cell per slot: writing a slot never borrows the ones readers hold.
    names: [UnsafeCell<[u8; L]>; N],
    lens: [AtomicU8; N],
    len: AtomicUsize,
    writer: Mutex<CriticalSectionRawMutex, ()>,
}

// SAFETY: entries below `len` are immutable; writes happen above it, under `writer`, and are
// published by the Release store of `len`.
unsafe impl<const N: usize, const L: usize> Sync for Symbols<N, L> {}

impl Symbols {
    /// 32 names of up to 24 bytes. For other sizes, `Symbols::<N, L>::sized()`.
    pub const fn new() -> Self {
        Self::sized()
    }
}

impl<const N: usize, const L: usize> Symbols<N, L> {
    pub const fn sized() -> Self {
        const { assert!(L <= u8::MAX as usize, "names are at most 255 bytes") };
        Self {
            names: [const { UnsafeCell::new([0; L]) }; N],
            lens: [const { AtomicU8::new(0) }; N],
            len: AtomicUsize::new(0),
            writer: Mutex::new(()),
        }
    }

    /// `s`'s id: the existing one, or the next free. `None` when all `N` are taken or `s` is
    /// longer than `L` bytes.
    pub fn intern(&self, s: &str) -> Option<u64> {
        if s.len() > L {
            return None;
        }
        self.writer.lock(|_| {
            let n = self.len.load(Acquire);
            if let Some(i) = (0..n).find(|&i| self.get(i) == s) {
                return Some(i as u64);
            }
            if n == N {
                return None;
            }
            // SAFETY: slot n is above len, so no reader looks at it; we hold the writer lock
            unsafe { (&mut *self.names[n].get())[..s.len()].copy_from_slice(s.as_bytes()) };
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
        let mut all = [""; N];
        for (i, slot) in all[..n].iter_mut().enumerate() {
            *slot = self.get(i);
        }
        f(&all[..n])
    }

    fn get(&self, i: usize) -> &str {
        let l = self.lens[i].load(Relaxed) as usize;
        // SAFETY: i < len, so the entry is immutable and was written from a &str
        unsafe { core::str::from_utf8_unchecked(&(&*self.names[i].get())[..l]) }
    }
}

impl<const N: usize, const L: usize> Default for Symbols<N, L> {
    fn default() -> Self {
        Self::sized()
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

    /// Under Miri: a reader holding names while another thread interns must not alias a `&mut`.
    #[test]
    fn intern_while_reading() {
        let s = Symbols::new();
        s.intern("a");
        s.with(|names| {
            std::thread::scope(|t| {
                t.spawn(|| s.intern("b"));
            });
            assert_eq!(names, ["a"]);
        });
        s.with(|n| assert_eq!(n, ["a", "b"]));
    }

    #[test]
    fn sized_to_the_application() {
        let s = Symbols::<2, 4>::sized();
        assert_eq!(s.intern("abcd"), Some(0));
        assert_eq!(s.intern("abcde"), None); // longer than 4
        assert_eq!(s.intern("x"), Some(1));
        assert_eq!(s.intern("y"), None); // full at 2
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
