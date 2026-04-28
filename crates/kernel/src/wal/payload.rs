use crate::format::bytes::{read_u32, read_u64, write_bytes, write_u32, write_u64};
use crate::format::{Csn, Lsn, PageId, RelId, RowId, TxId};
use crate::{Error, Result};

const TAG_HEAP_INSERT: u8 = 1;
const TAG_HEAP_UPDATE: u8 = 2;
const TAG_HEAP_DELETE: u8 = 3;
const TAG_COMMIT: u8 = 4;
const TAG_PAGE_IMAGE: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WalPayload {
    HeapInsert {
        tx_id: TxId,
        rel_id: RelId,
        row_id: RowId,
        payload: Vec<u8>,
    },
    HeapUpdate {
        tx_id: TxId,
        rel_id: RelId,
        row_id: RowId,
        payload: Vec<u8>,
    },
    HeapDelete {
        tx_id: TxId,
        rel_id: RelId,
        row_id: RowId,
    },
    Commit {
        tx_id: TxId,
        csn: Csn,
    },
    PageImage {
        page_id: PageId,
        page_lsn: Lsn,
        page_bytes: Vec<u8>,
    },
}

impl WalPayload {
    pub fn tx_id(&self) -> TxId {
        match self {
            Self::HeapInsert { tx_id, .. }
            | Self::HeapUpdate { tx_id, .. }
            | Self::HeapDelete { tx_id, .. }
            | Self::Commit { tx_id, .. } => *tx_id,
            Self::PageImage { .. } => TxId::ZERO,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            Self::HeapInsert {
                tx_id,
                rel_id,
                row_id,
                payload,
            } => encode_row_payload(TAG_HEAP_INSERT, *tx_id, *rel_id, *row_id, payload),
            Self::HeapUpdate {
                tx_id,
                rel_id,
                row_id,
                payload,
            } => encode_row_payload(TAG_HEAP_UPDATE, *tx_id, *rel_id, *row_id, payload),
            Self::HeapDelete {
                tx_id,
                rel_id,
                row_id,
            } => {
                let mut out = vec![0; 25];
                out[0] = TAG_HEAP_DELETE;
                write_u64(&mut out, 1, tx_id.0)?;
                write_u64(&mut out, 9, rel_id.0)?;
                write_u64(&mut out, 17, row_id.0)?;
                Ok(out)
            }
            Self::Commit { tx_id, csn } => {
                let mut out = vec![0; 17];
                out[0] = TAG_COMMIT;
                write_u64(&mut out, 1, tx_id.0)?;
                write_u64(&mut out, 9, csn.0)?;
                Ok(out)
            }
            Self::PageImage {
                page_id,
                page_lsn,
                page_bytes,
            } => {
                if page_bytes.len() > u32::MAX as usize {
                    return Err(Error::CorruptWal("page image too large"));
                }
                let mut out = vec![0; 21 + page_bytes.len()];
                out[0] = TAG_PAGE_IMAGE;
                write_u64(&mut out, 1, page_id.0)?;
                write_u64(&mut out, 9, page_lsn.0)?;
                write_u32(&mut out, 17, page_bytes.len() as u32)?;
                write_bytes(&mut out, 21, page_bytes)?;
                Ok(out)
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let tag = *bytes
            .first()
            .ok_or(Error::CorruptWal("empty wal payload"))?;
        match tag {
            TAG_HEAP_INSERT => {
                decode_row_payload(bytes, |tx_id, rel_id, row_id, payload| Self::HeapInsert {
                    tx_id,
                    rel_id,
                    row_id,
                    payload,
                })
            }
            TAG_HEAP_UPDATE => {
                decode_row_payload(bytes, |tx_id, rel_id, row_id, payload| Self::HeapUpdate {
                    tx_id,
                    rel_id,
                    row_id,
                    payload,
                })
            }
            TAG_HEAP_DELETE => {
                if bytes.len() >= 25 {
                    let rel_id = RelId(read_u64(bytes, 9)?);
                    let row_id = RowId(read_u64(bytes, 17)?);
                    require_exact_len(bytes, 25)?;
                    Ok(Self::HeapDelete {
                        tx_id: TxId(read_u64(bytes, 1)?),
                        rel_id,
                        row_id,
                    })
                } else {
                    require_exact_len(bytes, 17)?;
                    Ok(Self::HeapDelete {
                        tx_id: TxId(read_u64(bytes, 1)?),
                        rel_id: RelId::ZERO,
                        row_id: RowId(read_u64(bytes, 9)?),
                    })
                }
            }
            TAG_COMMIT => {
                require_exact_len(bytes, 17)?;
                Ok(Self::Commit {
                    tx_id: TxId(read_u64(bytes, 1)?),
                    csn: Csn(read_u64(bytes, 9)?),
                })
            }
            TAG_PAGE_IMAGE => {
                if bytes.len() < 21 {
                    return Err(Error::BufferTooSmall {
                        needed: 21,
                        actual: bytes.len(),
                    });
                }
                let image_len = read_u32(bytes, 17)? as usize;
                let expected = 21_usize
                    .checked_add(image_len)
                    .ok_or(Error::CorruptWal("page image length overflow"))?;
                require_exact_len(bytes, expected)?;
                Ok(Self::PageImage {
                    page_id: PageId(read_u64(bytes, 1)?),
                    page_lsn: Lsn(read_u64(bytes, 9)?),
                    page_bytes: bytes[21..expected].to_vec(),
                })
            }
            _ => Err(Error::CorruptWal("unknown wal payload tag")),
        }
    }
}

fn encode_row_payload(
    tag: u8,
    tx_id: TxId,
    rel_id: RelId,
    row_id: RowId,
    payload: &[u8],
) -> Result<Vec<u8>> {
    if payload.len() > u32::MAX as usize {
        return Err(Error::CorruptWal("heap payload too large"));
    }
    let mut out = vec![0; 29 + payload.len()];
    out[0] = tag;
    write_u64(&mut out, 1, tx_id.0)?;
    write_u64(&mut out, 9, rel_id.0)?;
    write_u64(&mut out, 17, row_id.0)?;
    write_u32(&mut out, 25, payload.len() as u32)?;
    write_bytes(&mut out, 29, payload)?;
    Ok(out)
}

fn decode_row_payload(
    bytes: &[u8],
    build: fn(TxId, RelId, RowId, Vec<u8>) -> WalPayload,
) -> Result<WalPayload> {
    if bytes.len() < 21 {
        return Err(Error::BufferTooSmall {
            needed: 21,
            actual: bytes.len(),
        });
    }
    if bytes.len() >= 29 {
        let payload_len = read_u32(bytes, 25)? as usize;
        let expected = 29_usize
            .checked_add(payload_len)
            .ok_or(Error::CorruptWal("heap payload length overflow"))?;
        if bytes.len() < expected {
            return Err(Error::BufferTooSmall {
                needed: expected,
                actual: bytes.len(),
            });
        }
        if expected == bytes.len() {
            return Ok(build(
                TxId(read_u64(bytes, 1)?),
                RelId(read_u64(bytes, 9)?),
                RowId(read_u64(bytes, 17)?),
                bytes[29..expected].to_vec(),
            ));
        }
    }
    let payload_len = read_u32(bytes, 17)? as usize;
    let expected = 21_usize
        .checked_add(payload_len)
        .ok_or(Error::CorruptWal("heap payload length overflow"))?;
    if bytes.len() < expected {
        return Err(Error::BufferTooSmall {
            needed: expected,
            actual: bytes.len(),
        });
    }
    if expected == bytes.len() {
        return Ok(build(
            TxId(read_u64(bytes, 1)?),
            RelId::ZERO,
            RowId(read_u64(bytes, 9)?),
            bytes[21..expected].to_vec(),
        ));
    }
    Err(Error::CorruptWal("heap payload length mismatch"))
}

fn require_exact_len(bytes: &[u8], expected: usize) -> Result<()> {
    if bytes.len() != expected {
        return Err(Error::BufferTooSmall {
            needed: expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}
