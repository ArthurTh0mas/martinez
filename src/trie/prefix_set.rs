use std::collections::BTreeSet;

#[derive(Clone)]
pub(crate) struct PrefixSet(BTreeSet<Vec<u8>>);

impl PrefixSet {
    pub(crate) fn new() -> Self {
        Self { 0: BTreeSet::new() }
    }

    pub(crate) fn contains(&mut self, prefix: &[u8]) -> bool {
        self.0
            .range(prefix.to_vec()..)
            .next()
            .map(|s| s.starts_with(prefix))
            .unwrap_or(false)
    }

    pub(crate) fn insert(&mut self, key: Vec<u8>) {
        self.0.insert(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_set() {
        let mut ps = PrefixSet::new();
        assert!(!ps.contains(b""));
        assert!(!ps.contains(b"a"));

        ps.insert(b"abc".to_vec());
        ps.insert(b"fg".to_vec());
        ps.insert(b"abc".to_vec()); // duplicate
        ps.insert(b"ab".to_vec());

        assert!(ps.contains(b""));
        assert!(ps.contains(b"a"));
        assert!(!ps.contains(b"aac"));
        assert!(ps.contains(b"ab"));
        assert!(ps.contains(b"abc"));
        assert!(!ps.contains(b"abcd"));
        assert!(!ps.contains(b"b"));
        assert!(ps.contains(b"f"));
        assert!(ps.contains(b"fg"));
        assert!(!ps.contains(b"fgk"));
        assert!(!ps.contains(b"fy"));
        assert!(!ps.contains(b"yyz"));
    }
}
