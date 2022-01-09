mod coordinator;
mod sentry;
pub mod types;

// impl PartialEq for HeadData {
//     fn eq(&self, other: &Self) -> bool {
//         self.height == other.height && self.hash == other.hash && self.td == other.td
//     }
// }

// impl Eq for HeadData {}

// pub struct AtomicHeadData(AtomicPtr<HeadData>);

// impl AtomicHeadData {
//     pub fn new() -> Self {
//         Self(AtomicPtr::new(Box::into_raw(Box::new(HeadData::default()))))
//     }

//     pub fn get(&self) -> HeadData {
//         unsafe { *self.0.load(atomic::Ordering::Relaxed) }
//     }

//     pub fn store(&self, data: HeadData) {
//         self.0
//             .store(Box::into_raw(Box::new(data)), atomic::Ordering::SeqCst);
//     }

//     pub fn compare_exchange(&self, current: HeadData, new: HeadData) -> bool {
//         let current_ptr = Box::into_raw(Box::new(current));
//         let new_ptr = Box::into_raw(Box::new(new));
//         self.0
//             .compare_exchange(
//                 current_ptr,
//                 new_ptr,
//                 atomic::Ordering::SeqCst,
//                 atomic::Ordering::SeqCst,
//             )
//             .is_ok()
//     }
// }
