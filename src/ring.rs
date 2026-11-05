use core::mem::MaybeUninit;

/// Drop-newest ring of fixed-size records. Records stay at the head until acked, so the sender can
/// re-send them after a reconnect while producers keep pushing. Size it from the board's memory.
pub struct Ring<T: Copy + 'static> {
    buf: &'static mut [MaybeUninit<T>],
    head: usize,
    len: usize,
    pub dropped: u32,
}

impl<T: Copy> Ring<T> {
    pub fn new(buf: &'static mut [MaybeUninit<T>]) -> Self {
        Self {
            buf,
            head: 0,
            len: 0,
            dropped: 0,
        }
    }

    pub fn push(&mut self, r: T) {
        if self.len == self.buf.len() {
            self.dropped += 1; // ponytail: drops newest when full, keeps in-flight head stable
            return;
        }
        let i = (self.head + self.len) % self.buf.len();
        self.buf[i].write(r);
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn get(&self, i: usize) -> Option<T> {
        // SAFETY: slots [head, head+len) were written by push
        (i < self.len)
            .then(|| unsafe { self.buf[(self.head + i) % self.buf.len()].assume_init_read() })
    }

    pub fn pop(&mut self, n: usize) {
        let n = n.min(self.len);
        self.head = (self.head + n) % self.buf.len();
        self.len -= n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_and_drops_newest_when_full() {
        let mut r = Ring::new(Box::leak(Box::new([MaybeUninit::<u32>::uninit(); 3])));
        (0..4).for_each(|i| r.push(i));
        assert_eq!((r.len(), r.dropped, r.get(2)), (3, 1, Some(2)));
        r.pop(2);
        (7..9).for_each(|i| r.push(i));
        assert_eq!([r.get(0), r.get(1), r.get(2)], [Some(2), Some(7), Some(8)]);
        assert_eq!(r.get(3), None);
        r.pop(9);
        assert!(r.is_empty());
    }
}
