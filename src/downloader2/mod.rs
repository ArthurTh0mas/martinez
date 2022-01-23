use crate::models::{BlockHeader, H256};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::RwLock as AsyncMutex;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Anchor {
    pub parent_hash: H256,
    pub height: u64,
    pub timestamp: u64,
    pub id: u64,
}

pub struct HeaderDownloader<'a> {
    pub bad_headers: Arc<AsyncMutex<HashSet<H256>>>,
    pub anchors: Arc<AsyncMutex<HashMap<H256, Anchor>>>,
    pub preverified_hashes: HashSet<H256>,
    pub links: HashMap<H256, Link<'a>>,
    pub insert_list: Vec<Link<'a>>,
    pub seen_announces: HashSet<H256>,
}
