use bytes::Bytes;

#[derive(Debug, Default)]
pub(crate) struct PrefixSet {
    keys: Vec<Bytes>,
    sorted: bool,
    index: usize,
}

impl PrefixSet {
    pub fn insert(&mut self, key: Bytes) {
        self.keys.push(key);
        self.sorted = false;
    }

    pub fn contains(&mut self, prefix: impl AsRef<[u8]>) -> bool {
        let prefix = prefix.as_ref();
        if self.keys.is_empty() {
            return false;
        }

        if !self.sorted {
            self.keys.sort_unstable();
            self.keys.dedup_by(|a, b| (*a).eq(b));
        }

        // We optimize for the case when contains() inquires are made with increasing prefixes,
        // e.g. contains("00"), contains("04"), contains("0b"), contains("0b05"), contains("0c"), contains("0f"), ...
        // instead of some random order.
        assert!(self.index < self.keys.len());
        while self.index > 0 && self.keys[self.index] > prefix {
            self.index -= 1;
        }

        loop {
            if self.keys[self.index].starts_with(prefix) {
                return true;
            }
            if self.keys[self.index] > prefix {
                return false;
            }
            if self.index == self.keys.len() - 1 {
                return false;
            }

            self.index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_set() {
        let mut ps = PrefixSet::default();

        assert!(!ps.contains(""));
        assert!(!ps.contains("a"));

        ps.insert("abc".as_bytes().to_vec().into());
        ps.insert("fg".as_bytes().to_vec().into());
        ps.insert("abc".as_bytes().to_vec().into()); // duplicate
        ps.insert("ab".as_bytes().to_vec().into());

        assert!(ps.contains(""));
        assert!(ps.contains("a"));
        assert!(!ps.contains("aac"));
        assert!(ps.contains("ab"));
        assert!(ps.contains("abc"));
        assert!(!ps.contains("abcd"));
        assert!(!ps.contains("b"));
        assert!(ps.contains("f"));
        assert!(ps.contains("fg"));
        assert!(!ps.contains("fgk"));
        assert!(!ps.contains("fy"));
        assert!(!ps.contains("yyz"));
    }
}
