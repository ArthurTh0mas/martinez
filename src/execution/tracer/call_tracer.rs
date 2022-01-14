use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub struct CallTracerFlags {
    pub from: bool,
    pub to: bool,
}

#[derive(Debug, Default)]
pub struct CallTracer {
    addresses: HashMap<Address, CallTracerFlags>,
}

impl Tracer for CallTracer {
    fn capture_start(
        &mut self,
        _: u16,
        from: Address,
        to: Address,
        _: MessageKind,
        _: Bytes,
        _: u64,
        _: U256,
    ) {
        self.addresses.entry(from).or_default().from = true;
        self.addresses.entry(to).or_default().to = true;
    }

    fn capture_self_destruct(&mut self, caller: Address, beneficiary: Address) {
        self.addresses.entry(caller).or_default().from = true;
        self.addresses.entry(beneficiary).or_default().to = true;
    }
}

impl CallTracer {
    pub fn into_sorted_iter(&self) -> impl Iterator<Item = (Address, CallTracerFlags)> {
        self.addresses
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
    }
}
