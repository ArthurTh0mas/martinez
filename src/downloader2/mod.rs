use crate::models::{BlockHeader, H256};
use std::collections::{HashMap, HashSet};

pub struct Link<'a> {
    pub header: BlockHeader,
    pub next: Option<&'a Link<'a>>,
    pub height: u64,
    pub hash: H256,
    pub persistent: bool,
    pub preverified: bool,
    pub index: u64,
}

impl<'a> Link<'a> {
    pub fn new(
        header: BlockHeader,
        next: Option<&'a Link<'a>>,
        height: u64,
        hash: H256,
        persistent: bool,
        preverified: bool,
        index: u64,
    ) -> Self {
        Self {
            header,
            next,
            height,
            hash,
            persistent,
            preverified,
            index,
        }
    }
}

pub struct LinkIter<'a>(&'a Link<'a>);

impl<'a> IntoIterator for &'a Link<'a> {
    type Item = &'a Link<'a>;
    type IntoIter = LinkIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LinkIter(self)
    }
}

impl<'a> Iterator for LinkIter<'a> {
    type Item = &'a Link<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.next {
            Some(next) => {
                self.0 = next;
                Some(self.0)
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone, Eq, Hash)]
pub struct Anchor {
    pub parent_hash: H256,
    pub height: u64,
    pub timestamp: u64,
    pub id: u64,
}

impl PartialEq for Anchor {
    fn eq(&self, other: &Self) -> bool {
        self.parent_hash == other.parent_hash && self.height == other.height
    }
}

impl PartialOrd for Anchor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Anchor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.timestamp == other.timestamp {
            return self.height.cmp(&other.height);
        }
        other.timestamp.cmp(&self.timestamp)
    }
}

pub struct HeaderDownloader<'a> {
    pub bad_headers: HashSet<H256>,
    pub anchors: HashMap<H256, Anchor>,
    pub preverified_hashes: HashSet<H256>,
    pub links: HashMap<H256, Link<'a>>,
    pub insert_list: Vec<Link<'a>>,
    pub seen_announces: HashSet<H256>,
    pub preverified_height: u64,
    pub actively_fetching: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BinaryHeap;
    #[test]
    fn it_works() {
        let mut heap = BinaryHeap::<Anchor>::new();
        let anchor2 = Anchor {
            parent_hash: H256::from_low_u64_be(2),
            height: 2,
            timestamp: 2,
            id: 2,
        };

        let anchor = Anchor {
            parent_hash: H256::default(),
            height: 0,
            timestamp: 0,
            id: 0,
        };
        let anchor1 = Anchor {
            parent_hash: H256::from_low_u64_be(1),
            height: 1,
            timestamp: 1,
            id: 1,
        };

        heap.push(anchor2.clone());
        heap.push(anchor.clone());
        heap.push(anchor1.clone());

        assert_eq!(heap.pop(), Some(anchor));
        assert_eq!(heap.pop(), Some(anchor1));
        assert_eq!(heap.pop(), Some(anchor2));
    }
}
