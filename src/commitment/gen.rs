use super::*;

pub(crate) type InnerCoroutine<'a, R> =
    Box<dyn Generator<ResumeData, Yield = InterruptData, Return = R> + Send + Sync + Unpin + 'a>;

pub struct StartedInterrupt<'a, R> {
    pub(crate) inner: InnerCoroutine<'a, R>,
}

impl<'a, R> StartedInterrupt<'a, R> {
    fn resume(self) -> Interrupt<'a, R> {
        resume_interrupt(self.inner, ResumeData::Empty)
    }
}

pub struct LoadBranchInterrupt<'a, R> {
    pub(crate) inner: InnerCoroutine<'a, R>,
}

impl<'a, R> LoadBranchInterrupt<'a, R> {
    fn resume(self, resume_data: BranchData) -> Interrupt<'a, R> {
        resume_interrupt(self.inner, ResumeData::BranchData(resume_data))
    }
}
pub struct LoadAccountInterrupt<'a, R> {
    pub(crate) inner: InnerCoroutine<'a, R>,
}

impl<'a, R> LoadAccountInterrupt<'a, R> {
    fn resume(self, resume_data: FilledAccount) -> Interrupt<'a, R> {
        resume_interrupt(self.inner, ResumeData::FilledAccount(resume_data))
    }
}

pub struct LoadStorageInterrupt<'a, R> {
    pub(crate) inner: InnerCoroutine<'a, R>,
}

impl<'a, R> LoadStorageInterrupt<'a, R> {
    fn resume(self, resume_data: FilledStorage) -> Interrupt<'a, R> {
        resume_interrupt(self.inner, ResumeData::FilledStorage(resume_data))
    }
}

pub struct BranchUpdateInterrupt<'a, R> {
    pub(crate) inner: InnerCoroutine<'a, R>,
}

impl<'a, R> BranchUpdateInterrupt<'a, R> {
    fn resume(self) -> Interrupt<'a, R> {
        resume_interrupt(self.inner, ResumeData::Empty)
    }
}

#[derive(From, Debug)]
pub struct BranchData(pub Vec<u8>);
#[derive(From, Debug)]
pub struct FilledAccount(pub Cell);
#[derive(From, Debug)]
pub struct FilledStorage(pub Cell);

#[derive(From, Debug)]
pub enum ResumeData {
    Empty,
    BranchData(BranchData),
    FilledAccount(FilledAccount),
    FilledStorage(FilledStorage),
}
pub struct Complete<'a, R>(pub(crate) InnerCoroutine<'a, R>);

pub enum Interrupt<'a, R> {
    LoadBranch {
        interrupt: LoadBranchInterrupt<'a, R>,
        prefix: Vec<u8>,
    },
    LoadAccount {
        interrupt: LoadAccountInterrupt<'a, R>,
        plain_key: Vec<u8>,
        cell: Cell,
    },
    LoadStorage {
        interrupt: LoadStorageInterrupt<'a, R>,
        plain_key: Vec<u8>,
        cell: Cell,
    },
    BranchUpdate {
        interrupt: BranchUpdateInterrupt<'a, R>,
        update_key: Vec<u8>,
        branch_node: Vec<u8>,
    },
    Complete {
        interrupt: Complete<'a, R>,
        result: R,
    },
}

pub enum InterruptData {
    LoadBranch {
        prefix: Vec<u8>,
    },
    LoadAccount {
        plain_key: Vec<u8>,
        cell: Cell,
    },
    LoadStorage {
        plain_key: Vec<u8>,
        cell: Cell,
    },
    BranchUpdate {
        update_key: Vec<u8>,
        branch_node: Vec<u8>,
    },
}

fn resume_interrupt<R>(
    mut inner: InnerCoroutine<'_, R>,
    resume_data: ResumeData,
) -> Interrupt<'_, R> {
    match Pin::new(&mut *inner).resume(resume_data) {
        GeneratorState::Yielded(interrupt_data) => match interrupt_data {
            InterruptData::LoadBranch { prefix } => Interrupt::LoadBranch {
                interrupt: LoadBranchInterrupt { inner },
                prefix,
            },
            InterruptData::LoadAccount { plain_key, cell } => Interrupt::LoadAccount {
                interrupt: LoadAccountInterrupt { inner },
                plain_key,
                cell,
            },
            InterruptData::LoadStorage { plain_key, cell } => Interrupt::LoadStorage {
                interrupt: LoadStorageInterrupt { inner },
                plain_key,
                cell,
            },
            InterruptData::BranchUpdate {
                update_key,
                branch_node,
            } => Interrupt::BranchUpdate {
                interrupt: BranchUpdateInterrupt { inner },
                update_key,
                branch_node,
            },
        },
        GeneratorState::Complete(result) => Interrupt::Complete {
            interrupt: Complete(inner),
            result,
        },
    }
}
