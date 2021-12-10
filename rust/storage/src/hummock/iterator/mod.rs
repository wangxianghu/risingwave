use super::{HummockResult, HummockValue};

mod concat;
pub use concat::*;
mod sorted;
pub use sorted::*;
mod user_key;
pub use user_key::*;

#[cfg(test)]
mod test_utils;

use async_trait::async_trait;

/// `HummockIterator` defines the interface of all iterators, including `TableIterator`,
/// `SortedIterator`, `UserKeyIterator` and `ConcatIterator`.
#[async_trait]
pub trait HummockIterator {
    /// Get the next key/value pair in the table. If `None` is returned, the iterator must be
    /// rewinded to retrieve new values.
    async fn next(&mut self) -> HummockResult<()>;

    fn key(&self) -> HummockResult<&[u8]>;

    fn value(&self) -> HummockResult<HummockValue<&[u8]>>;

    fn is_valid(&self) -> bool {
        self.key().is_ok()
    }

    /// Reset the position of the iterator
    async fn rewind(&mut self) -> HummockResult<()>;

    /// Seek will reset iterator and seek to the first position where the key >= provided key.
    async fn seek(&mut self, key: &[u8]) -> HummockResult<()>;
}

#[allow(clippy::large_enum_variant)]
pub enum HummockIteratorImpl {
    // Will support later
    // Table(TableIterator),
    Concat(Box<ConcatIterator>),
    Sorted(SortedIterator),
    #[cfg(test)]
    Test(test_utils::TestIterator),
}
