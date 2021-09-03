pub mod gen;
pub mod rlputil;

use self::rlputil::*;
use crate::{crypto::keccak256, models::*, u256_to_h256, zeroless_view};
use array_macro::array;
use arrayvec::ArrayVec;
use bytes::{BufMut, BytesMut};
use derive_more::From;
use gen::*;
use sha3::{Digest, Keccak256};
use std::{
    collections::HashMap,
    ops::{Generator, GeneratorState},
    pin::Pin,
    ptr::addr_of_mut,
};
use tracing::trace;

#[derive(Clone, Debug)]
pub struct Cell {
    h: Option<H256>,              // Cell hash
    apk: Option<Address>,         // account plain key
    spk: Option<(Address, H256)>, // storage plain key
    down_hashed_key: ArrayVec<u8, 128>,
    extension: ArrayVec<u8, 64>,
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: H256, // hash of the bytecode
    pub storage: Option<U256>,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            h: None,
            apk: None,
            spk: None,
            down_hashed_key: Default::default(),
            extension: Default::default(),
            nonce: Default::default(),
            balance: Default::default(),
            code_hash: EMPTY_HASH,
            storage: Default::default(),
        }
    }
}

impl Cell {
    fn compute_hash_len(&self, depth: usize) -> usize {
        if self.spk.is_some() && depth >= 64 {
            let key_len = 128 - depth + 1; // Length of hex key with terminator character
            let compact_len = (key_len - 1) / 2 + 1;
            let (kp, kl) = if compact_len > 1 {
                (1, compact_len)
            } else {
                (0, 1)
            };
            let storage_val = self.storage.map(u256_to_h256).unwrap_or_default();
            let val = RlpSerializableBytes(zeroless_view(&storage_val));
            let total_len = kp + kl + val.double_rlp_len();
            let pt = generate_struct_len(total_len).len();
            if total_len + pt < KECCAK_LENGTH {
                return total_len + pt;
            }
        }
        KECCAK_LENGTH + 1
    }

    // fn account_for_hashing(&self, storage_root_hash: H256) -> ArrayVec<u8, 128> {
    //     let mut buffer = ArrayVec::new();

    //     let mut balanceBytes = 0;
    //     if self.balance >= 128 {
    //         balanceBytes = ((U256::BITS - self.balance.leading_zeros() + 7) / 8) as u8;
    //     }

    //     let mut nonceBytes = 0;
    //     if self.nonce < 128 && self.nonce != 0 {
    //         nonceBytes = 0;
    //     } else {
    //         nonceBytes = (((U256::BITS - self.nonce.leading_zeros()) + 7) / 8) as u8;
    //     }

    //     let mut structLength = balanceBytes + nonceBytes + 2;
    //     structLength += 66; // Two 32-byte arrays + 2 prefixes

    //     if structLength < 56 {
    //         buffer.try_push(192 + structLength).unwrap();
    //     } else {
    //         let lengthBytes = ((u8::BITS - structLength.leading_zeros() + 7) / 8) as u8;
    //         buffer.try_push(247 + lengthBytes).unwrap();

    //         let mut i = lengthBytes;
    //         while i > 0 {
    //             buffer.try_push(structLength as u8);
    //             structLength >>= 8;
    //             i -= 1;
    //         }
    //     }

    // // Encoding nonce
    // if cell.Nonce < 128 && cell.Nonce != 0 {
    // 	buffer[pos] = byte(cell.Nonce)
    // } else {
    // 	buffer[pos] = byte(128 + nonceBytes)
    // 	var nonce = cell.Nonce
    // 	for i := nonceBytes; i > 0; i-- {
    // 		buffer[pos+i] = byte(nonce)
    // 		nonce >>= 8
    // 	}
    // }
    // pos += 1 + nonceBytes

    // // Encoding balance
    // if cell.Balance.LtUint64(128) && !cell.Balance.IsZero() {
    // 	buffer[pos] = byte(cell.Balance.Uint64())
    // 	pos++
    // } else {
    // 	buffer[pos] = byte(128 + balanceBytes)
    // 	pos++
    // 	cell.Balance.WriteToSlice(buffer[pos : pos+balanceBytes])
    // 	pos += balanceBytes
    // }

    // // Encoding Root and CodeHash
    // buffer[pos] = 128 + 32
    // pos++
    // copy(buffer[pos:], storageRootHash[:length.Hash])
    // pos += 32
    // buffer[pos] = 128 + 32
    // pos++
    // copy(buffer[pos:], cell.CodeHash[:])
    // pos += 32
    // return pos

    //     buffer
    // }
}

#[derive(Debug)]
struct CellGrid {
    root: Cell, // Root cell of the tree
    // Rows of the grid correspond to the level of depth in the patricia tree
    // Columns of the grid correspond to pointers to the nodes further from the root
    grid: [[Cell; 16]; 128], // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
}

impl Default for CellGrid {
    fn default() -> Self {
        Self {
            root: Cell::default(),
            grid: array![array![Cell::default(); 16]; 128],
        }
    }
}

impl CellGrid {
    #[inline(always)]
    fn cell_mut(&mut self, cell_position: Option<CellPosition>) -> &mut Cell {
        if let Some(position) = cell_position {
            self.grid_cell_mut(position)
        } else {
            &mut self.root
        }
    }

    #[inline(always)]
    fn grid_cell_mut(&mut self, cell_position: CellPosition) -> &mut Cell {
        &mut self.grid[cell_position.row as usize][cell_position.col as usize]
    }

    #[inline(always)]
    fn cell_mut_ptr(&mut self, cell_position: Option<CellPosition>) -> *mut Cell {
        if let Some(position) = cell_position {
            self.grid_cell_mut(position)
        } else {
            addr_of_mut!(self.root)
        }
    }

    #[inline(always)]
    fn grid_cell_mut_ptr(&mut self, cell_position: CellPosition) -> *mut Cell {
        addr_of_mut!(self.grid[cell_position.row as usize][cell_position.col as usize])
    }

    fn fill_from_upper_cell(
        &mut self,
        cell: Option<CellPosition>,
        up_cell: Option<CellPosition>,
        depth: usize,
        depth_increment: usize,
    ) {
        let up_cell = self.cell_mut(up_cell).clone();
        let cell = self.cell_mut(cell);

        cell.down_hashed_key.clear();
        if up_cell.down_hashed_key.len() > depth_increment {
            cell.down_hashed_key
                .try_extend_from_slice(&up_cell.down_hashed_key[depth_increment..])
                .unwrap();
        }
        cell.extension.clear();
        if up_cell.extension.len() > depth_increment {
            cell.extension
                .try_extend_from_slice(&up_cell.extension[depth_increment..])
                .unwrap();
        }
        if depth <= 64 {
            cell.apk = up_cell.apk;
            if up_cell.apk.is_some() {
                cell.balance = up_cell.balance;
                cell.nonce = up_cell.nonce;
                cell.code_hash = up_cell.code_hash;
                cell.extension = up_cell.extension;
            }
        } else {
            cell.apk = None;
        }
        cell.spk = up_cell.spk;
        if up_cell.spk.is_some() {
            cell.storage = up_cell.storage;
        }
        cell.h = up_cell.h;
    }

    fn fill_from_lower_cell(
        &mut self,
        cell: Option<CellPosition>,
        low_cell: CellPosition,
        low_depth: usize,
        pre_extension: &[u8],
        nibble: usize,
    ) {
        let low_cell = self.grid_cell_mut(low_cell).clone();
        let cell = self.cell_mut(cell);

        if low_cell.apk.is_some() || low_depth < 64 {
            cell.apk = low_cell.apk;
        }
        if low_cell.apk.is_some() {
            cell.balance = low_cell.balance;
            cell.nonce = low_cell.nonce;
            cell.code_hash = low_cell.code_hash;
        }
        cell.spk = low_cell.spk;
        if low_cell.spk.is_some() {
            cell.storage = low_cell.storage;
        }
        if low_cell.h.is_some() {
            if (low_cell.apk.is_none() && low_depth < 64)
                || (low_cell.spk.is_none() && low_depth > 64)
            {
                // Extension is related to either accounts branch node, or storage branch node, we prepend it by preExtension | nibble
                cell.extension.clear();
                cell.extension.try_extend_from_slice(pre_extension).unwrap();
                cell.extension.push(nibble as u8);
                cell.extension
                    .try_extend_from_slice(&low_cell.extension)
                    .unwrap();
            } else {
                // Extension is related to a storage branch node, so we copy it upwards as is
                cell.extension = low_cell.extension;
            }
        }
        cell.h = low_cell.h;
    }
}

fn hash_key(plain_key: &[u8], hashed_key_offset: usize) -> ArrayVec<u8, 32> {
    let hash_buf = keccak256(plain_key).0;
    let mut hash_buf = &hash_buf[hashed_key_offset / 2..];
    let mut dest = ArrayVec::new();
    if hashed_key_offset % 2 == 1 {
        dest.push(hash_buf[0] & 0xf);
        hash_buf = &hash_buf[1..];
    }
    for c in hash_buf {
        dest.push((c >> 4) & 0xf);
        dest.push(c & 0xf);
    }

    dest
}

/// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
/// with keys pre-hashed by keccak256
#[derive(Debug)]
pub struct HexPatriciaHashed {
    grid: CellGrid,
    // How many rows (starting from row 0) are currently active and have corresponding selected columns
    // Last active row does not have selected column
    active_rows: usize,
    // Length of the key that reflects current positioning of the grid. It maybe larger than number of active rows,
    // if a account leaf cell represents multiple nibbles in the key
    current_key: ArrayVec<u8, 128>, // For each row indicates which column is currently selected
    depths: [usize; 128],           // For each row, the depth of cells in that row
    root_checked: bool, // Set to false if it is not known whether the root is empty, set to true if it is checked
    root_mod: bool,
    root_del: bool,
    before_bitmap: [u16; 128], // For each row, bitmap of cells that were present before modification
    mod_bitmap: [u16; 128],    // For each row, bitmap of cells that were modified (not deleted)
    del_bitmap: [u16; 128],    // For each row, bitmap of cells that were deleted
    // Function used to load branch node and fill up the cells
    // For each cell, it sets the cell type, clears the modified flag, fills the hash,
    // and for the extension, account, and leaf type, the `l` and `k`
    // branchFn: Box<dyn Fn(prefix: &[u8]) -> func(prefix []byte) ([]byte, error)
    // Function used to fetch account with given plain key. It loads
    // accountFn func(plainKey []byte, cell *Cell) error
    // Function used to fetch account with given plain key
    // storageFn       func(plainKey []byte, cell *Cell) error
    // keccak          keccakState
    // keccak2         keccakState
    account_key_len: usize,
    byte_array_writer: BytesMut,
    key_prefix: ArrayVec<u8, 1>,
    val_buf: [u8; 128], // Enough to accommodate hash encoding of any account
    prefix_buf: [u8; 8],
}

impl Default for HexPatriciaHashed {
    fn default() -> Self {
        Self {
            grid: Default::default(),
            active_rows: Default::default(),
            current_key: Default::default(),
            depths: [0; 128],
            root_checked: Default::default(),
            root_mod: Default::default(),
            root_del: Default::default(),
            before_bitmap: [0; 128],
            mod_bitmap: [0; 128],
            del_bitmap: [0; 128],
            account_key_len: Default::default(),
            byte_array_writer: Default::default(),
            key_prefix: Default::default(),
            val_buf: [0; 128],
            prefix_buf: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct CellPosition {
    row: usize,
    col: usize,
}

#[derive(Clone, Debug)]
pub struct UpdateFlags {
    pub code: bool,
    pub delete: bool,
    pub balance: bool,
    pub nonce: bool,
    pub storage: bool,
}

#[derive(Clone, Debug)]
pub struct Update {
    pub flags: UpdateFlags,
    pub balance: U256,
    pub nonce: u64,
    pub code_hash_or_storage: [u8; 32],
    pub val_length: usize,
}

#[derive(Clone, Debug)]
pub struct ProcessUpdateArg {
    pub hashed_key: H256,
    pub plain_key: Vec<u8>,
    pub update: Update,
}

impl HexPatriciaHashed {
    pub fn root_hash(&mut self) -> H256 {
        if let Some(root) = self.grid.cell_mut(None).h {
            root
        } else {
            self.compute_cell_hash(None, 0)
        }
    }

    pub fn process_updates(
        &mut self,
        updates: Vec<ProcessUpdateArg>,
    ) -> StartedInterrupt<'_, HashMap<Vec<u8>, Vec<u8>>> {
        let inner = move |_| {
            let mut branch_node_updates = HashMap::new();

            for ProcessUpdateArg {
                hashed_key,
                plain_key,
                update,
            } in updates
            {
                trace!(
                    "plain_key={:?}, hashed_key={:?}, current_key={:?}, update={:?}",
                    plain_key,
                    hashed_key,
                    hex::encode(&self.current_key),
                    update
                );

                // Keep folding until the currentKey is the prefix of the key we modify
                while self.need_folding(hashed_key) {
                    let (branch_node_update, update_key) = self.fold();
                    if let Some(branch_node_update) = branch_node_update {
                        branch_node_updates.insert(update_key, branch_node_update);
                    }
                }
            }

            yield InterruptData::LoadBranch { prefix: vec![] };

            branch_node_updates
        };

        StartedInterrupt {
            inner: Box::new(inner),
        }
    }

    fn compute_cell_hash(&mut self, pos: Option<CellPosition>, depth: usize) -> H256 {
        let hash = EMPTY_ROOT;

        let cell = self.grid.cell_mut(pos);
        let mut storage_root = None;
        if let Some((address, location)) = cell.spk {
            let mut spk = [0; 52];
            // ????
            spk[..20].copy_from_slice(&address.0);
            spk[20..].copy_from_slice(&location.0);
            let hashed_key_offset = depth.saturating_sub(64);
            let singleton = depth <= 64;
            cell.down_hashed_key.clear();
            cell.down_hashed_key
                .try_extend_from_slice(&hash_key(&spk, hashed_key_offset))
                .unwrap();
            cell.down_hashed_key[64 - hashed_key_offset] = 16; // Add terminator
            if singleton {
                trace!(
                    "leafHashWithKeyVal(singleton) for [{}]=>[{:?}]",
                    hex::encode(&cell.down_hashed_key[..64 - hashed_key_offset + 1]),
                    cell.storage
                );
                storage_root = Some(H256::from_slice(
                    &leaf_hash_with_key_val(
                        &cell.down_hashed_key[..64 - hashed_key_offset + 1],
                        RlpSerializableBytes(&cell.storage.unwrap().to_be_bytes()),
                        true,
                    )[1..],
                ));
            } else {
                trace!(
                    "leafHashWithKeyVal for [{}]=>[{:?}]",
                    hex::encode(&cell.down_hashed_key[..64 - hashed_key_offset + 1]),
                    cell.storage
                );
                return H256::from_slice(&leaf_hash_with_key_val(
                    &cell.down_hashed_key[..64 - hashed_key_offset + 1],
                    RlpSerializableBytes(&cell.storage.unwrap().to_be_bytes()),
                    false,
                ));
            }
        }
        if let Some(apk) = cell.apk {
            cell.down_hashed_key.clear();
            cell.down_hashed_key
                .try_extend_from_slice(&hash_key(&apk.0, depth))
                .unwrap();
            cell.down_hashed_key[64 - depth] = 16; // Add terminator

            let storage_root = storage_root.unwrap_or_else(|| {
                if !cell.extension.is_empty() {
                    // Extension
                    let h = cell.h.expect("computeCellHash extension without hash");
                    trace!(
                        "extension_hash for [{}]=>[{:?}]\n",
                        hex::encode(&cell.extension),
                        h
                    );
                    extension_hash(&cell.extension, h)
                } else if let Some(h) = cell.h {
                    h
                } else {
                    EMPTY_ROOT
                }
            });
            let account_rlp = rlp::encode(&RlpAccount {
                storage_root,
                nonce: cell.nonce,
                balance: cell.balance,
                code_hash: cell.code_hash,
            });
            trace!(
                "accountLeafHashWithKey for [{}]=>[{}]\n",
                hex::encode(&cell.down_hashed_key[..65 - depth]),
                hex::encode(&account_rlp)
            );
            account_leaf_hash_with_key(
                &cell.down_hashed_key[..65 - depth],
                RlpEncodableBytes(&account_rlp),
            );
        }
        // buf := []byte{0x80 + 32}
        // if cell.extLen > 0 {
        //     // Extension
        //     if cell.hl > 0 {
        //         if hph.trace {
        //             fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.h[:cell.hl])
        //         }
        //         if buf, err = hph.extensionHash(buf, cell.extension[:cell.extLen], cell.h[:cell.hl]); err != nil {
        //             return nil, err
        //         }
        //     } else {
        //         return nil, fmt.Errorf("computeCellHash extension without hash")
        //     }
        // } else if cell.hl > 0 {
        //     buf = append(buf, cell.h[:cell.hl]...)
        // } else {
        //     buf = append(buf, EmptyRootHash...)
        // }
        // return buf, nil

        hash
    }

    fn need_folding(&self, hashed_key: H256) -> bool {
        !hashed_key[..].starts_with(&self.current_key[..])
    }

    pub(crate) fn fold(&mut self) -> (Option<Vec<u8>>, Vec<u8>) {
        let update_key = hex_to_compact(&self.current_key[..]);
        assert_ne!(self.active_rows, 0, "cannot fold - no active rows");
        trace!(
            "fold: activeRows: {}, currentKey: [{:?}], modBitmap: {:#018b}, delBitmap: {:#018b}",
            self.active_rows,
            hex::encode(&self.current_key[..]),
            self.mod_bitmap[self.active_rows - 1],
            self.del_bitmap[self.active_rows - 1]
        );
        // Move information to the row above
        let row = self.active_rows - 1;
        let mut col = 0;
        let mut up_depth = 0;
        let up_cell = if self.active_rows == 1 {
            trace!("upcell is root");

            None
        } else {
            up_depth = self.depths[self.active_rows - 2];
            col = self.current_key[up_depth - 1];

            trace!("upcell is ({} x {}), upDepth={}", row - 1, col, up_depth);

            Some(CellPosition {
                row: row - 1,
                col: col as usize,
            })
        };
        let depth = self.depths[self.active_rows - 1];
        let mut branch_data = None;
        trace!(
            "beforeBitmap[{}]={:#018b}, modBitmap[{}]={:#018b}, delBitmap[{}]={:#018b}",
            row,
            self.before_bitmap[row],
            row,
            self.mod_bitmap[row],
            row,
            self.del_bitmap[row]
        );

        let bitmap = (self.before_bitmap[row] | self.mod_bitmap[row]) ^ self.del_bitmap[row];
        let parts_count = bitmap.count_ones();
        match parts_count {
            0 => {
                // Everything deleted
                if self.del_bitmap[row] != 0 {
                    if row == 0 {
                        self.root_del = true;
                    } else if up_depth != 64 {
                        self.del_bitmap[row - 1] |= 1_u16 << col;
                        trace!(
                            "del delBitmap[{}]={:#018b}",
                            row - 1,
                            self.del_bitmap[row - 1]
                        )
                    }
                }
                self.grid.cell_mut(up_cell).h = None;
                self.grid.cell_mut(up_cell).apk = None;
                self.grid.cell_mut(up_cell).spk = None;
                self.grid.cell_mut(up_cell).extension.clear();
                self.grid.cell_mut(up_cell).down_hashed_key.clear();
                if self.before_bitmap[row].count_ones() > 1 {
                    // Deletion
                    branch_data = Some(vec![]);
                }
                self.active_rows -= 1;
                if up_depth > 0 {
                    self.current_key.truncate(up_depth - 1);
                }
            }
            1 => {
                // Leaf or extension node
                if self.mod_bitmap[row] != 0 || self.del_bitmap[row] != 0 {
                    // any modifications
                    if row == 0 {
                        self.root_mod = true;
                    } else {
                        self.mod_bitmap[row - 1] |= 1_u16 << col;
                        self.del_bitmap[row - 1] &= !(1_u16 << col);
                        trace!(
                            "leaf/ext modBitmap[{}]={:#018b}, delBitmap[{}]={:#018b}",
                            row - 1,
                            self.mod_bitmap[row - 1],
                            row - 1,
                            self.del_bitmap[row - 1]
                        );
                    }
                }
                let nibble = bitmap.trailing_zeros().try_into().unwrap();
                self.grid.cell_mut(up_cell).extension.clear();
                self.grid.fill_from_lower_cell(
                    up_cell,
                    CellPosition { row, col: nibble },
                    depth,
                    &self.current_key[up_depth..],
                    nibble,
                );
                if self.before_bitmap[row].count_ones() > 1 {
                    // Deletion
                    branch_data = Some(vec![])
                }
                self.active_rows -= 1;

                if let Some(new_current_key_len) = up_depth.checked_sub(1) {
                    self.current_key.truncate(new_current_key_len);
                } else {
                    self.current_key.clear();
                }
            }
            _ => {
                // Branch node
                if self.mod_bitmap[row] != 0 || self.del_bitmap[row] != 0 {
                    // any modifications
                    if row == 0 {
                        self.root_mod = true
                    } else {
                        self.mod_bitmap[row - 1] |= 1_u16 << col;
                        self.del_bitmap[row - 1] &= !(1_u16 << col);
                        trace!(
                            "branch modBitmap[{}]={:#018b}, delBitmap[{}]={:#018b}",
                            row - 1,
                            self.mod_bitmap[row - 1],
                            row - 1,
                            self.del_bitmap[row - 1],
                        );
                    }
                }
                // Calculate total length of all hashes
                let mut total_branch_len = 17 - parts_count as usize; // for every empty cell, one byte
                let mut bitset = bitmap;
                while bitset != 0 {
                    let bit = bitset & 0_u16.overflowing_sub(bitset).0;
                    let nibble = bit.trailing_zeros() as usize;
                    total_branch_len += self
                        .grid
                        .cell_mut(Some(CellPosition { row, col: nibble }))
                        .compute_hash_len(depth);
                    bitset ^= bit;
                }
                // Parts bitmap
                let mut branch_data = branch_data.get_or_insert_with(Vec::new);
                branch_data.extend_from_slice(&bitmap.to_be_bytes());
                let fields_pos = 2;
                // Add field flags
                let zeroes = (parts_count + 1) / 2;

                if zeroes > 0 {
                    for _ in 0..zeroes {
                        branch_data.push(0);
                    }
                }

                let mut hasher = Keccak256::new();
                hasher.update(&rlputil::generate_struct_len(total_branch_len));
                trace!("branchHash [{}] {{", hex::encode(&update_key));
                let mut last_nibble = 0;
                let mut bitset = bitmap;
                let mut j = 0;
                while bitset != 0 {
                    let bit = bitset & 0_u16.overflowing_sub(bitset).0;
                    let nibble = bit.trailing_zeros() as usize;
                    for i in last_nibble..nibble {
                        hasher.update(&[0x80]);
                        trace!("{}: empty({},{})", i, row, i);
                    }
                    last_nibble = nibble + 1;
                    let cell_pos = CellPosition { row, col: nibble };
                    let cell_hash = self.compute_cell_hash(Some(cell_pos), depth);
                    let cell = self.grid.grid_cell_mut(cell_pos);
                    trace!(
                        "{}: computeCellHash({},{},depth={})=[{:?}]",
                        nibble,
                        row,
                        nibble,
                        depth,
                        cell_hash
                    );
                    //     if _, err = hph.keccak2.Write(cellHash); err != nil {
                    //         return nil, nil, err
                    //     }
                    //     var fieldBits PartFlags
                    //     if cell.extLen > 0 && cell.spl == 0 {
                    //         fieldBits |= HASHEDKEY_PART
                    //         n := binary.PutUvarint(hph.numBuf[:], uint64(cell.extLen))
                    //         branchData = append(branchData, hph.numBuf[:n]...)
                    //         branchData = append(branchData, cell.extension[:cell.extLen]...)
                    //     }
                    //     if cell.apl > 0 {
                    //         fieldBits |= ACCOUNT_PLAIN_PART
                    //         n := binary.PutUvarint(hph.numBuf[:], uint64(cell.apl))
                    //         branchData = append(branchData, hph.numBuf[:n]...)
                    //         branchData = append(branchData, cell.apk[:cell.apl]...)
                    //     }
                    //     if cell.spl > 0 {
                    //         fieldBits |= STORAGE_PLAIN_PART
                    //         n := binary.PutUvarint(hph.numBuf[:], uint64(cell.spl))
                    //         branchData = append(branchData, hph.numBuf[:n]...)
                    //         branchData = append(branchData, cell.spk[:cell.spl]...)
                    //     }
                    //     if cell.hl > 0 {
                    //         fieldBits |= HASH_PART
                    //         n := binary.PutUvarint(hph.numBuf[:], uint64(cell.hl))
                    //         branchData = append(branchData, hph.numBuf[:n]...)
                    //         branchData = append(branchData, cell.h[:cell.hl]...)
                    //     }
                    //     if j%2 == 1 {
                    //         fieldBits <<= 4
                    //     }
                    //     branchData[fieldsPos+(j/2)] |= byte(fieldBits)
                    bitset ^= bit;

                    j += 1;
                }
                // for i := lastNibble; i < 17; i++ {
                //     if _, err := hph.keccak2.Write(&[0x80]); err != nil {
                //         return nil, nil, err
                //     }
                //     if hph.trace {
                //         fmt.Printf("%x: empty(%d,%x)\n", i, row, i)
                //     }
                // }
                // upCell.extLen = depth - upDepth - 1
                // if upCell.extLen > 0 {
                //     copy(upCell.extension[:], hph.currentKey[upDepth:hph.currentKeyLen])
                // }
                // if depth < 64 {
                //     upCell.apl = 0
                // }
                // upCell.spl = 0
                // upCell.hl = 32
                // if _, err := hph.keccak2.Read(upCell.h[:]); err != nil {
                //     return nil, nil, err
                // }
                // if hph.trace {
                //     fmt.Printf("} [%x]\n", upCell.h[:])
                // }
                // hph.activeRows--
                // if upDepth > 0 {
                //     hph.currentKeyLen = upDepth - 1
                // } else {
                //     hph.currentKeyLen = 0
                // }
            }
        }
        // if branchData != nil {
        //     if hph.trace {
        //         fmt.Printf("fold: update key: %x\n", updateKey)
        //     }
        // }
        (branch_data, update_key)
    }
}

fn make_compact_zero_byte(key: &[u8]) -> (u8, usize, usize) {
    let mut compact_zero_byte = 0_u8;
    let mut key_pos = 0_usize;
    let mut key_len = key.len();
    // todo: strip suffix
    if has_term(key) {
        key_len -= 1;
        compact_zero_byte = 0x20;
    }
    let first_nibble = key.first().copied().unwrap_or(0);
    if key_len & 1 == 1 {
        compact_zero_byte |= 0x10 | first_nibble; // Odd: (1<<4) + first nibble
        key_pos += 1
    }

    (compact_zero_byte, key_pos, key_len)
}

fn has_term(s: &[u8]) -> bool {
    s.last().map(|&v| v == 16).unwrap_or(false)
}

fn hex_to_compact(key: &[u8]) -> Vec<u8> {
    let (zero_byte, key_pos, key_len) = make_compact_zero_byte(key);
    let buf_len = key_len / 2 + 1; // always > 0
    let mut buf = vec![0; buf_len];
    buf[0] = zero_byte;

    let key = &key[..key_pos];
    let mut key_len = key.len();
    if has_term(key) {
        key_len -= 1;
    }

    let mut key_index = 0;
    let mut buf_index = 1;
    while key_index < key_len {
        key_index += 2;
        buf_index += 1;

        if key_index == key_len - 1 {
            buf[buf_index] &= 0x0f
        } else {
            buf[buf_index] = key[key_index + 1]
        }
        buf[buf_index] |= key[key_index] << 4
    }

    buf
}

fn account_leaf_hash_with_key(key: &[u8], val: impl RlpSerializable) -> H256 {
    // // Compute the total length of binary representation
    // var kp, kl int
    // // Write key
    // var compactLen int
    // var ni int
    // var compact0 byte
    // if hasTerm(key) {
    // 	compactLen = (len(key)-1)/2 + 1
    // 	if len(key)&1 == 0 {
    // 		compact0 = 48 + key[0] // Odd (1<<4) + first nibble
    // 		ni = 1
    // 	} else {
    // 		compact0 = 32
    // 	}
    // } else {
    // 	compactLen = len(key)/2 + 1
    // 	if len(key)&1 == 1 {
    // 		compact0 = 16 + key[0] // Odd (1<<4) + first nibble
    // 		ni = 1
    // 	}
    // }
    // if compactLen > 1 {
    // 	hph.keyPrefix[0] = byte(128 + compactLen)
    // 	kp = 1
    // 	kl = compactLen
    // } else {
    // 	kl = 1
    // }
    // var err error
    // var buf []byte
    // if buf, err = hph.completeLeafHash(kp, kl, compactLen, key, compact0, ni, val, true); err != nil {
    // 	return nil, err
    // }
    // return buf, nil
    todo!()
}

fn extension_hash(key: &[u8], hash: H256) -> H256 {
    // Compute the total length of binary representation
    // Write key
    let mut compact_len = 0;
    let mut ni = 0;
    let mut compact0 = 0;
    if has_term(key) {
        compact_len = (key.len() - 1) / 2 + 1;
        if key.len() & 1 == 0 {
            compact0 = 0x30 + key[0]; // Odd: (3<<4) + first nibble
            ni = 1;
        } else {
            compact0 = 0x20;
        }
    } else {
        compact_len = key.len() / 2 + 1;
        if key.len() & 1 == 1 {
            compact0 = 0x10 + key[0]; // Odd: (1<<4) + first nibble
            ni = 1;
        }
    }
    let (kp, kl) = if compact_len > 1 {
        (Some(0x80 + compact_len as u8), compact_len)
    } else {
        (None, 1)
    };
    let total_len = if kp.is_some() { 1 } else { 0 } + kl + 33;

    let mut hasher = Keccak256::new();
    hasher.update(&generate_struct_len(total_len));
    if let Some(kp) = kp {
        hasher.update(&[kp]);
    }
    hasher.update(&[compact0]);
    if compact_len > 1 {
        for i in 1..compact_len {
            hasher.update(&[key[ni] * 16 + key[ni + 1]]);
            ni += 2
        }
    }
    hasher.update(&[0x80 + KECCAK_LENGTH as u8]);
    hasher.update(&hash[..]);
    // Replace previous hash with the new one
    H256::from_slice(&hasher.finalize())
}

fn complete_leaf_hash(
    kp: Option<u8>,
    kl: usize,
    compact_len: usize,
    key: &[u8],
    compact0: u8,
    mut ni: usize,
    val: impl rlputil::RlpSerializable,
    singleton: bool,
) -> Vec<u8> {
    let total_len = if kp.is_some() { 1 } else { 0 } + kl + val.double_rlp_len();
    let len_prefix = generate_struct_len(total_len);
    let embedded = !singleton && total_len + len_prefix.len() < KECCAK_LENGTH;

    if embedded {
        let mut buf = Vec::new();
        buf.put_slice(&len_prefix);
        if let Some(kp) = kp {
            buf.put_u8(kp);
        }
        buf.put_u8(compact0);
        for i in 1..compact_len {
            buf.put_u8(key[ni] * 16 + key[ni + 1]);
            ni += 2
        }
        let mut buf = buf.writer();
        val.to_double_rlp(&mut buf);
        buf.into_inner()
    } else {
        let mut hasher = Keccak256::new();
        hasher.update(&len_prefix);
        if let Some(kp) = kp {
            hasher.update(&[kp]);
        }
        hasher.update(&[compact0]);
        for i in 1..compact_len {
            hasher.update(&[key[ni] * 16 + key[ni + 1]]);
            ni += 2;
        }
        val.to_double_rlp(&mut hasher);
        let mut hash_buf = [0; 33];
        hash_buf[0] = 0x80;
        hash_buf[1..].copy_from_slice(&hasher.finalize());
        hash_buf.to_vec()
    }
}

fn leaf_hash_with_key_val(
    key: &[u8],
    val: rlputil::RlpSerializableBytes<'_>,
    singleton: bool,
) -> Vec<u8> {
    // Compute the total length of binary representation
    // Write key
    let compact_len = key.len() / 2 + 1;
    let (compact0, ni) = if key.len() & 1 == 0 {
        (0x30 + key[0], 1) // Odd: (3<<4) + first nibble
    } else {
        (0x20, 0)
    };
    let (kp, kl) = if compact_len > 1 {
        (Some(0x80 + compact_len as u8), compact_len)
    } else {
        (None, 1)
    };
    complete_leaf_hash(kp, kl, compact_len, key, compact0, ni, val, singleton)
}
