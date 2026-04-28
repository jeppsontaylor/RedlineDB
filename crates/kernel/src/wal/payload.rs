use crate::format::bytes::{read_u32, read_u64, write_bytes, write_u32, write_u64};
use crate::format::{BackupId, Csn, Lsn, PageId, RelId, RowId, TimelineId, TxId, WalSegmentNo};
use crate::{Error, Result};

const TAG_HEAP_INSERT: u8 = 1;
const TAG_HEAP_UPDATE: u8 = 2;
const TAG_HEAP_DELETE: u8 = 3;
const TAG_COMMIT: u8 = 4;
const TAG_PAGE_IMAGE: u8 = 5;
const TAG_SEGMENT_SEAL: u8 = 6;
const TAG_BACKUP_BEGIN: u8 = 7;
const TAG_BACKUP_END: u8 = 8;
const TAG_TIMELINE_FORK: u8 = 9;
const TAG_LOGICAL_TXN: u8 = 10;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalEvent {
    Relation {
        rel_id: RelId,
        schema_epoch: u64,
    },
    Insert {
        rel_id: RelId,
        row_id: RowId,
        values: Vec<Vec<u8>>,
    },
    Update {
        rel_id: RelId,
        row_id: RowId,
        values: Vec<Vec<u8>>,
    },
    Delete {
        rel_id: RelId,
        row_id: RowId,
    },
    Ddl {
        schema_epoch: u64,
    },
}

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
    SegmentSeal {
        timeline: TimelineId,
        segment_no: WalSegmentNo,
        first_lsn: Lsn,
        last_lsn: Lsn,
        crc32: u32,
    },
    BackupBegin {
        backup_id: BackupId,
        required_wal_start: Lsn,
    },
    BackupEnd {
        backup_id: BackupId,
        stop_lsn: Lsn,
        stop_csn: Csn,
    },
    TimelineFork {
        parent: TimelineId,
        fork_lsn: Lsn,
        child: TimelineId,
    },
    LogicalTxn {
        csn: Csn,
        first_lsn: Lsn,
        events: Vec<LogicalEvent>,
    },
}

impl WalPayload {
    pub fn tx_id(&self) -> TxId {
        match self {
            Self::HeapInsert { tx_id, .. }
            | Self::HeapUpdate { tx_id, .. }
            | Self::HeapDelete { tx_id, .. }
            | Self::Commit { tx_id, .. } => *tx_id,
            Self::PageImage { .. }
            | Self::SegmentSeal { .. }
            | Self::BackupBegin { .. }
            | Self::BackupEnd { .. }
            | Self::TimelineFork { .. }
            | Self::LogicalTxn { .. } => TxId::ZERO,
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
            Self::SegmentSeal {
                timeline,
                segment_no,
                first_lsn,
                last_lsn,
                crc32,
            } => {
                let mut out = vec![0; 41];
                out[0] = TAG_SEGMENT_SEAL;
                write_u64(&mut out, 1, timeline.0)?;
                write_u64(&mut out, 9, segment_no.0)?;
                write_u64(&mut out, 17, first_lsn.0)?;
                write_u64(&mut out, 25, last_lsn.0)?;
                write_u32(&mut out, 33, *crc32)?;
                Ok(out)
            }
            Self::BackupBegin {
                backup_id,
                required_wal_start,
            } => {
                let mut out = vec![0; 25];
                out[0] = TAG_BACKUP_BEGIN;
                write_u64(&mut out, 1, (backup_id.0 >> 64) as u64)?;
                write_u64(&mut out, 9, backup_id.0 as u64)?;
                write_u64(&mut out, 17, required_wal_start.0)?;
                Ok(out)
            }
            Self::BackupEnd {
                backup_id,
                stop_lsn,
                stop_csn,
            } => {
                let mut out = vec![0; 33];
                out[0] = TAG_BACKUP_END;
                write_u64(&mut out, 1, (backup_id.0 >> 64) as u64)?;
                write_u64(&mut out, 9, backup_id.0 as u64)?;
                write_u64(&mut out, 17, stop_lsn.0)?;
                write_u64(&mut out, 25, stop_csn.0)?;
                Ok(out)
            }
            Self::TimelineFork {
                parent,
                fork_lsn,
                child,
            } => {
                let mut out = vec![0; 25];
                out[0] = TAG_TIMELINE_FORK;
                write_u64(&mut out, 1, parent.0)?;
                write_u64(&mut out, 9, fork_lsn.0)?;
                write_u64(&mut out, 17, child.0)?;
                Ok(out)
            }
            Self::LogicalTxn {
                csn,
                first_lsn,
                events,
            } => {
                let mut out = vec![0; 17];
                out[0] = TAG_LOGICAL_TXN;
                write_u64(&mut out, 1, csn.0)?;
                write_u64(&mut out, 9, first_lsn.0)?;
                push_u64(&mut out, events.len() as u64);
                for event in events {
                    encode_logical_event(event, &mut out)?;
                }
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
            TAG_SEGMENT_SEAL => {
                require_exact_len(bytes, 41)?;
                Ok(Self::SegmentSeal {
                    timeline: TimelineId(read_u64(bytes, 1)?),
                    segment_no: WalSegmentNo(read_u64(bytes, 9)?),
                    first_lsn: Lsn(read_u64(bytes, 17)?),
                    last_lsn: Lsn(read_u64(bytes, 25)?),
                    crc32: read_u32(bytes, 33)?,
                })
            }
            TAG_BACKUP_BEGIN => {
                require_exact_len(bytes, 25)?;
                Ok(Self::BackupBegin {
                    backup_id: read_backup_id(bytes, 1)?,
                    required_wal_start: Lsn(read_u64(bytes, 17)?),
                })
            }
            TAG_BACKUP_END => {
                require_exact_len(bytes, 33)?;
                Ok(Self::BackupEnd {
                    backup_id: read_backup_id(bytes, 1)?,
                    stop_lsn: Lsn(read_u64(bytes, 17)?),
                    stop_csn: Csn(read_u64(bytes, 25)?),
                })
            }
            TAG_TIMELINE_FORK => {
                require_exact_len(bytes, 25)?;
                Ok(Self::TimelineFork {
                    parent: TimelineId(read_u64(bytes, 1)?),
                    fork_lsn: Lsn(read_u64(bytes, 9)?),
                    child: TimelineId(read_u64(bytes, 17)?),
                })
            }
            TAG_LOGICAL_TXN => {
                if bytes.len() < 25 {
                    return Err(Error::BufferTooSmall {
                        needed: 25,
                        actual: bytes.len(),
                    });
                }
                let csn = Csn(read_u64(bytes, 1)?);
                let first_lsn = Lsn(read_u64(bytes, 9)?);
                let mut cursor = 17;
                let event_count = read_u64(bytes, cursor)? as usize;
                cursor += 8;
                let mut events = Vec::with_capacity(event_count);
                for _ in 0..event_count {
                    let (event, used) = decode_logical_event(&bytes[cursor..])?;
                    cursor += used;
                    events.push(event);
                }
                require_exact_len(bytes, cursor)?;
                Ok(Self::LogicalTxn {
                    csn,
                    first_lsn,
                    events,
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

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_backup_id(bytes: &[u8], offset: usize) -> Result<BackupId> {
    let high = read_u64(bytes, offset)? as u128;
    let low = read_u64(bytes, offset + 8)? as u128;
    Ok(BackupId((high << 64) | low))
}

fn encode_logical_event(event: &LogicalEvent, out: &mut Vec<u8>) -> Result<()> {
    match event {
        LogicalEvent::Relation {
            rel_id,
            schema_epoch,
        } => {
            out.push(1);
            push_u64(out, rel_id.0);
            push_u64(out, *schema_epoch);
        }
        LogicalEvent::Insert {
            rel_id,
            row_id,
            values,
        } => {
            out.push(2);
            push_u64(out, rel_id.0);
            push_u64(out, row_id.0);
            push_u64(out, values.len() as u64);
            for value in values {
                push_u64(out, value.len() as u64);
                out.extend_from_slice(value);
            }
        }
        LogicalEvent::Update {
            rel_id,
            row_id,
            values,
        } => {
            out.push(3);
            push_u64(out, rel_id.0);
            push_u64(out, row_id.0);
            push_u64(out, values.len() as u64);
            for value in values {
                push_u64(out, value.len() as u64);
                out.extend_from_slice(value);
            }
        }
        LogicalEvent::Delete { rel_id, row_id } => {
            out.push(4);
            push_u64(out, rel_id.0);
            push_u64(out, row_id.0);
        }
        LogicalEvent::Ddl { schema_epoch } => {
            out.push(5);
            push_u64(out, *schema_epoch);
        }
    }
    Ok(())
}

fn decode_logical_event(bytes: &[u8]) -> Result<(LogicalEvent, usize)> {
    if bytes.is_empty() {
        return Err(Error::BufferTooSmall {
            needed: 1,
            actual: 0,
        });
    }

    let mut cursor = 1;
    let event = match bytes[0] {
        1 => {
            let rel_id = RelId(read_u64(bytes, cursor)?);
            cursor += 8;
            let schema_epoch = read_u64(bytes, cursor)?;
            cursor += 8;
            LogicalEvent::Relation {
                rel_id,
                schema_epoch,
            }
        }
        2 | 3 => {
            let rel_id = RelId(read_u64(bytes, cursor)?);
            cursor += 8;
            let row_id = RowId(read_u64(bytes, cursor)?);
            cursor += 8;
            let count = read_u64(bytes, cursor)? as usize;
            cursor += 8;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u64(bytes, cursor)? as usize;
                cursor += 8;
                let end = cursor
                    .checked_add(len)
                    .ok_or(Error::CorruptWal("logical payload overflow"))?;
                if bytes.len() < end {
                    return Err(Error::BufferTooSmall {
                        needed: end,
                        actual: bytes.len(),
                    });
                }
                values.push(bytes[cursor..end].to_vec());
                cursor = end;
            }
            if bytes[0] == 2 {
                LogicalEvent::Insert {
                    rel_id,
                    row_id,
                    values,
                }
            } else {
                LogicalEvent::Update {
                    rel_id,
                    row_id,
                    values,
                }
            }
        }
        4 => {
            let rel_id = RelId(read_u64(bytes, cursor)?);
            cursor += 8;
            let row_id = RowId(read_u64(bytes, cursor)?);
            cursor += 8;
            LogicalEvent::Delete { rel_id, row_id }
        }
        5 => {
            let schema_epoch = read_u64(bytes, cursor)?;
            cursor += 8;
            LogicalEvent::Ddl { schema_epoch }
        }
        _ => {
            return Err(Error::CorruptWal("unknown logical event tag"));
        }
    };

    Ok((event, cursor))
}
